import os
import json
import time
import datetime
from typing import List, Tuple
import requests
import firebase_admin
from firebase_admin import credentials, firestore

from utils.constants import CYBERGOV_PARAMS, NETWORK_MAP

# ---------------------------------------------------------------------
# FIRESTORE INIT
# ---------------------------------------------------------------------

if not firebase_admin._apps:
    raw = os.getenv("FIREBASE_CREDENTIALS_JSON")
    if not raw:
        raise RuntimeError("FIREBASE_CREDENTIALS_JSON missing")

    raw = raw.strip()
    if raw.startswith('"') and raw.endswith('"'):
        raw = raw[1:-1]
    raw = raw.replace('\\"', '"')

    try:
        cred_dict = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"Bad FIREBASE_CREDENTIALS_JSON: {e}")

    cred = credentials.Certificate(cred_dict)
    firebase_admin.initialize_app(cred)

db = firestore.client()


# ---------------------------------------------------------------------
# LOG
# ---------------------------------------------------------------------

def log(msg: str):
    print(f"[DETECTOR] {msg}", flush=True)


# ---------------------------------------------------------------------
# FETCH ACTIVE PROPOSALS
# ---------------------------------------------------------------------

ACTIVE_STATUSES = [
    "DecisionDepositPlaced",
    "Submitted",
    "Deciding",
    "ConfirmStarted",
]

def fetch_active_proposals(network: str, retries: int = 3) -> List[int]:
    """
    Fetch up to 50 active proposals (we assume active < 50).
    """
    base = NETWORK_MAP[network]
    status_params = "&".join([f"status={s}" for s in ACTIVE_STATUSES])
    url = f"{base}?page=1&limit=50&{status_params}"

    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=25)
            r.raise_for_status()
            items = r.json().get("items", [])
            ids = []

            for it in items:
                try:
                    idx = it["onChainInfo"]["index"]
                    ids.append(int(idx))
                except Exception as e:
                    log(f"[{network}] parse error: {e}")

            if len(ids) >= 50:
                log(f"[{network}] ⚠️ WARNING: Hit 50-item limit")

            return ids

        except Exception as e:
            log(f"[{network}] fetch attempt {attempt} failed: {e}")
            if attempt == retries:
                raise
            time.sleep(2 ** attempt)

    return []


# ---------------------------------------------------------------------
# TRANSACTION LOGIC (Correct Signature!)
# ---------------------------------------------------------------------

@firestore.transactional
def ensure_scheduled_transaction(transaction, ref, now, network, pid, fresh_hours):
    """
    Firestore atomic logic:

    NEW:
        schedule_new
    FAILED:
        retry immediately
    SUCCESS < fresh_hours:
        skip
    SUCCESS >= fresh_hours:
        reschedule
    PENDING:
        reschedule
    """
    now_iso = now.isoformat().replace("+00:00", "Z")
    snap = ref.get(transaction=transaction)

    # ------------------- NEW PROPOSAL ---------------------
    if not snap.exists:
        transaction.set(ref, {
            "network": network,
            "proposalId": pid,
            "status": "discovered",
            "createdAt": firestore.SERVER_TIMESTAMP,
            "updatedAt": firestore.SERVER_TIMESTAMP,
            "createdBy": "cybergov-detector",
            "lastModifiedBy": "cybergov-detector",
            "version": 1,
            "scrapeStatus": "pending",
            "scrapeScheduled": True,
            "scrapeLastAttemptIso": now_iso,
            "discoveredAtIso": now_iso,
        })
        return "schedule_new"

    # ------------------- EXISTING -------------------------
    data = snap.to_dict() or {}
    status = data.get("scrapeStatus")
    last_iso = data.get("scrapeLastAttemptIso")

    # FAILED → Always retry
    if status == "failed":
        transaction.update(ref, {
            "scrapeStatus": "pending",
            "scrapeScheduled": True,
            "scrapeLastAttemptIso": now_iso,
            "updatedAt": firestore.SERVER_TIMESTAMP,
        })
        return "schedule_existing"

    # SUCCESS → Check freshness
    if last_iso and status == "success":
        try:
            last_dt = datetime.datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=datetime.timezone.utc)
            age = (now - last_dt).total_seconds() / 3600
            if age < fresh_hours:
                return "skip"
        except Exception as e:
            log(f"Timestamp parse error: {e}")

    # PENDING or stale → reschedule
    transaction.update(ref, {
        "scrapeStatus": "pending",
        "scrapeScheduled": True,
        "scrapeLastAttemptIso": now_iso,
        "updatedAt": firestore.SERVER_TIMESTAMP,
    })
    return "schedule_existing"


# ---------------------------------------------------------------------
# TRIGGER SCRAPER
# ---------------------------------------------------------------------

def trigger_scraper(network: str, pid: int) -> bool:
    repo = os.getenv("SCRAPER_REPO")
    workflow = os.getenv("SCRAPER_WORKFLOW", "scraper.yml")
    token = os.getenv("GITHUB_TOKEN")

    url = f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"

    try:
        r = requests.post(
            url,
            json={
                "ref": "main",
                "inputs": {
                    "network": network,
                    "proposal_id": str(pid)
                }
            },
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            },
            timeout=20,
        )

        if r.status_code not in (200, 201, 204):
            log(f"SCRAPER ERROR {network}-{pid}: {r.status_code} body={r.text}")
            return False

        return True

    except Exception as e:
        log(f"Trigger failed for {network}-{pid}: {e}")
        return False


# ---------------------------------------------------------------------
# SAVE SKIPPED
# ---------------------------------------------------------------------

def save_skipped(skipped: List[Tuple[str, int]], now: datetime.datetime):
    if not skipped:
        return

    now_iso = now.isoformat().replace("+00:00", "Z")

    db.collection("meta").document("detector_state").set(
        {
            "lastSkipped": [{"network": n, "proposalId": p} for (n, p) in skipped],
            "updatedAtIso": now_iso,
        },
        merge=True,
    )


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    log("Detector started.")

    networks = os.getenv("DETECTOR_NETWORKS", "polkadot,kusama,paseo").split(",")
    fresh_hours = float(os.getenv("DETECTOR_FRESH_HOURS", "24"))
    max_dispatch = int(os.getenv("DETECTOR_MAX_DISPATCH", "30"))
    min_ids = CYBERGOV_PARAMS.get("min_proposal_id", {})

    dispatch_count = 0
    skipped = []

    for network in networks:
        network = network.strip()
        if not network or network not in NETWORK_MAP:
            continue

        log(f"Fetching active proposals for {network}")

        try:
            ids = fetch_active_proposals(network)
        except Exception as e:
            log(f"Failed fetching proposals for {network}: {e}")
            continue

        # Min proposal filter
        min_id = int(min_ids.get(network, 0))
        ids = [pid for pid in ids if pid >= min_id]

        log(f"[{network}] {len(ids)} active proposals (>= {min_id})")

        for pid in ids:

            if dispatch_count >= max_dispatch:
                skipped.append((network, pid))
                continue

            ref = db.collection("proposals").document(f"{network}-{pid}")

            try:
                action = ensure_scheduled_transaction(
                    db.transaction(), ref, now, network, pid, fresh_hours
                )

            except Exception as e:
                log(f"Transaction failed for {network}-{pid}: {e}")
                skipped.append((network, pid))
                continue

            # Skip fresh success
            if action == "skip":
                continue

            # Trigger scraper
            ok = trigger_scraper(network, pid)
            if not ok:
                ref.set({
                    "scrapeStatus": "failed",
                    "scrapeScheduled": False,
                    "updatedAt": firestore.SERVER_TIMESTAMP,
                }, merge=True)
                skipped.append((network, pid))
                continue

            dispatch_count += 1
            time.sleep(0.15)  # Anti-rate-limit

    # Save skipped
    save_skipped(skipped, now)

    log(f"Detector complete. Dispatched {dispatch_count}.")
    if skipped:
        log(f"Skipped {len(skipped)} proposals.")


if __name__ == "__main__":
    main()
