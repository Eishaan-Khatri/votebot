# src/detector.py

import os
import sys
import httpx
import datetime
from typing import List, Set, Optional, Tuple

from google.cloud import firestore

# ---------- CONFIG (env-driven) ----------

NETWORK_DOMAIN = {
    "polkadot": "polkadot.polkassembly.io",
    "kusama": "kusama.polkassembly.io",
    "paseo": "paseo.polkassembly.io",
}

ACTIVE_STATUSES = [
    "DecisionDepositPlaced",
    "Submitted",
    "Deciding",
    "ConfirmStarted",
]

# Comma-separated list, e.g. "polkadot,kusama,paseo"
NETWORKS = [n.strip() for n in os.getenv("DETECTOR_NETWORKS", "paseo").split(",") if n.strip()]

FRESH_HOURS = int(os.getenv("DETECTOR_FRESH_HOURS", "24"))
MAX_DISPATCH_PER_RUN = int(os.getenv("DETECTOR_MAX_DISPATCH", "30"))

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")  # must be set in GitHub Actions
SCRAPER_WORKFLOW = os.getenv("SCRAPER_WORKFLOW", "scraper.yml")
SCRAPER_REPO = os.getenv("SCRAPER_REPO", "OWNER/REPO")  # e.g. "eishaan-khatri/cybergov"

SCRAPER_DISPATCH_URL = (
    f"https://api.github.com/repos/{SCRAPER_REPO}/actions/workflows/{SCRAPER_WORKFLOW}/dispatches"
)

# Firestore client
db = firestore.Client()


# ---------- UTIL ----------

def _log(msg: str):
    print(f"[detector] {msg}", flush=True)


def fetch_active_proposals(network: str, retries: int = 3) -> List[int]:
    """Fetch all active proposal IDs from Polkassembly v2."""
    domain = NETWORK_DOMAIN[network]
    base_url = f"https://{domain}/api/v2/ReferendumV2"
    page = 1
    limit = 50
    ids: Set[int] = set()

    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(timeout=20.0) as client:
                while True:
                    params = [("page", page), ("limit", limit)]
                    for s in ACTIVE_STATUSES:
                        params.append(("status", s))

                    r = client.get(base_url, params=params)
                    r.raise_for_status()
                    payload = r.json()
                    items = payload.get("data", [])
                    if not items:
                        break

                    for item in items:
                        pid = item.get("post_id") or item.get("proposalId") or item.get("id")
                        if pid is not None:
                            ids.add(int(pid))

                    total = payload.get("total", 0)
                    if page * limit >= total:
                        break
                    page += 1
            break
        except Exception as e:
            _log(f"[{network}] Polkassembly fetch failed (attempt {attempt}): {e}")
            if attempt == retries:
                raise

    return sorted(ids)


def is_fresh(last_fetched_at: Optional[datetime.datetime]) -> bool:
    """True if last_fetched_at < FRESH_HOURS ago."""
    if not last_fetched_at:
        return False
    if isinstance(last_fetched_at, datetime.datetime):
        ts = last_fetched_at.replace(tzinfo=None)
    else:
        ts = datetime.datetime.fromisoformat(str(last_fetched_at)).replace(tzinfo=None)
    return (datetime.datetime.utcnow() - ts) < datetime.timedelta(hours=FRESH_HOURS)


def trigger_scraper(network: str, proposal_id: int) -> bool:
    """Dispatch GitHub Action for scraper; returns True if success."""
    if not GITHUB_TOKEN:
        raise RuntimeError("GITHUB_TOKEN not set")

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
    }
    payload = {
        "ref": "main",
        "inputs": {
            "network": network,
            "proposal_id": str(proposal_id),
        },
    }

    resp = httpx.post(SCRAPER_DISPATCH_URL, headers=headers, json=payload, timeout=20.0)
    ok = 200 <= resp.status_code < 300
    if not ok:
        _log(f"[{network}-{proposal_id}] GitHub dispatch failed: {resp.status_code} {resp.text}")
    return ok


# ---------- FIRESTORE TRANSACTION HELPERS ----------

@firestore.transactional
def ensure_scheduled_transaction(
    transaction: firestore.Transaction,
    ref: firestore.DocumentReference,
    now: datetime.datetime,
    network: str,
    pid: int,
) -> str:
    """
    Atomically decide what to do:
    - "schedule" → caller should trigger scraper
    - "skip"    → nothing to do
    """
    snap = ref.get(transaction=transaction)

    if not snap.exists:
        # Brand new proposal
        transaction.set(ref, {
            "network": network,
            "proposalId": pid,
            "status": "discovered",
            "discovered_at": now,
            "last_fetched_at": None,
            "scrape_scheduled": True,
            "scrape_status": "pending",  # scraper will set success/failed
        })
        return "schedule"

    data = snap.to_dict()
    last_fetched = data.get("last_fetched_at")
    scrape_scheduled = data.get("scrape_scheduled", False)
    scrape_status = data.get("scrape_status")  # None | "pending" | "success" | "failed"

    # If already fresh and successfully scraped, skip
    if is_fresh(last_fetched) and scrape_status == "success":
        return "skip"

    # If last run failed → reschedule
    if scrape_status == "failed":
        transaction.update(ref, {
            "scrape_scheduled": True,
            "scrape_status": "pending",
        })
        return "schedule"

    # If stale or never scraped → (re)schedule
    if not is_fresh(last_fetched):
        transaction.update(ref, {
            "scrape_scheduled": True,
            "scrape_status": "pending",
        })
        return "schedule"

    # Fresh but pending scheduled → let it ride
    if scrape_scheduled and scrape_status == "pending":
        return "skip"

    # Default: be safe, (re)schedule
    transaction.update(ref, {
        "scrape_scheduled": True,
        "scrape_status": "pending",
    })
    return "schedule"


@firestore.transactional
def force_schedule_transaction(
    transaction: firestore.Transaction,
    ref: firestore.DocumentReference,
    now: datetime.datetime,
    network: str,
    pid: int,
) -> None:
    """
    Manual override: always (re)mark as pending and scheduled.
    """
    snap = ref.get(transaction=transaction)
    if not snap.exists:
        transaction.set(ref, {
            "network": network,
            "proposalId": pid,
            "status": "discovered",
            "discovered_at": now,
            "last_fetched_at": None,
            "scrape_scheduled": True,
            "scrape_status": "pending",
        })
    else:
        transaction.update(ref, {
            "scrape_scheduled": True,
            "scrape_status": "pending",
        })


# ---------- SKIPPED QUEUE HELPERS ----------

def load_skipped_queue() -> List[Tuple[str, int]]:
    meta_ref = db.collection("meta").document("detector")
    snap = meta_ref.get()
    if not snap.exists:
        return []
    data = snap.to_dict() or {}
    items = data.get("last_skipped", [])
    result: List[Tuple[str, int]] = []
    for item in items:
        try:
            result.append((item["network"], int(item["proposalId"])))
        except Exception:
            continue
    return result


def save_skipped_queue(queue: List[Tuple[str, int]], now: datetime.datetime) -> None:
    meta_ref = db.collection("meta").document("detector")
    to_store = [{"network": n, "proposalId": p} for (n, p) in queue]
    meta_ref.set({"last_skipped": to_store, "updated_at": now}, merge=True)


# ---------- MAIN DETECTOR LOGIC ----------

def process_candidate(
    network: str,
    pid: int,
    now: datetime.datetime,
    dispatch_count: int,
) -> Tuple[int, Optional[Tuple[str, int]]]:
    """
    Try to schedule scraper for (network, pid) within dispatch limits.
    Returns:
        new_dispatch_count, skipped_candidate (None or (network, pid) if we couldn't send)
    """
    if dispatch_count >= MAX_DISPATCH_PER_RUN:
        return dispatch_count, (network, pid)

    doc_id = f"{network}-{pid}"
    ref = db.collection("proposals").document(doc_id)
    transaction = db.transaction()
with transaction:
    action = ensure_scheduled_transaction(transaction, ref, now, network, pid)
    if action == "skip":
        return dispatch_count, None

    # schedule
    ok = trigger_scraper(network, pid)
    if not ok:
        # mark failed so detector can retry next run
        ref.update({"scrape_status": "failed"})
        return dispatch_count, (network, pid)

    return dispatch_count + 1, None


def run_detector():
    now = datetime.datetime.utcnow()
    dispatch_count = 0
    remaining_skipped: List[Tuple[str, int]] = []

    # 1) Process previously skipped candidates first
    skipped = load_skipped_queue()
    if skipped:
        _log(f"Processing {len(skipped)} previously skipped proposals")
    for (net, pid) in skipped:
        if net not in NETWORK_DOMAIN:
            continue
        dispatch_count, skipped_again = process_candidate(net, pid, now, dispatch_count)
        if skipped_again:
            remaining_skipped.append(skipped_again)

    # 2) Process current active proposals
    for network in NETWORKS:
        if network not in NETWORK_DOMAIN:
            _log(f"Unknown network in DETECTOR_NETWORKS: {network}")
            continue

        _log(f"Scanning active proposals for network={network}")
        try:
            active_ids = fetch_active_proposals(network)
        except Exception as e:
            _log(f"[{network}] failed to fetch active proposals: {e}")
            continue

        _log(f"[{network}] active proposals: {len(active_ids)}")

        for pid in active_ids:
            dispatch_count, skipped_now = process_candidate(network, pid, now, dispatch_count)
            if skipped_now:
                remaining_skipped.append(skipped_now)

    # 3) Save any that we didn't manage to dispatch
    if remaining_skipped:
        _log(f"Saving {len(remaining_skipped)} proposals to skipped queue for next run")
    save_skipped_queue(remaining_skipped, now)
    _log(f"Detector run complete. Dispatched {dispatch_count} scraper jobs.")


# ---------- MANUAL OVERRIDE ENTRYPOINT ----------

def process_single(network: str, proposal_id: int):
    """Manual override: always force schedule scraper for single proposal."""
    if network not in NETWORK_DOMAIN:
        raise ValueError(f"Unknown network: {network}")

    now = datetime.datetime.utcnow()
    doc_id = f"{network}-{proposal_id}"
    ref = db.collection("proposals").document(doc_id)
    transaction = db.transaction()
    force_schedule_transaction(transaction, ref, now, network, proposal_id)

    ok = trigger_scraper(network, proposal_id)
    if not ok:
        ref.update({"scrape_status": "failed"})
    _log(f"Manual override processed for {network}-{proposal_id}, ok={ok}")


# ---------- CLI ----------

if __name__ == "__main__":
    # Usage:
    #   python detector.py                -> normal scheduled run
    #   python detector.py <network> <proposal_id> -> manual override
    if len(sys.argv) == 3:
        n = sys.argv[1]
        p = int(sys.argv[2])
        process_single(n, p)
    else:
        run_detector()
