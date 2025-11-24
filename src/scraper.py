# src/scraper.py
"""
CyberGov / VoteBot — Production Scraper

Responsibilities:
- Load Firestore credentials from Prefect Secret "firebase-credentials-json"
- Fetch proposal JSON + on-chain metadata from Polkassembly ReferendumV2 API
- Merge on-chain metadata into rawData (including OnChain_MetaData + trackNumber)
- Archive previous Firestore document into proposals/<id>/versions/*
- Save rawData into proposals/<network>-<proposalId>.files.rawData
- Validate track against ALLOWED_TRACK_IDS
- If track allowed:
    - Generate MAGIS content via generate_content_for_magis()
    - Store files.content
    - Mark scrapeStatus = "success", scrapeScheduled = False, scrapeLastAttemptIso = now
    - trackAllowed = True, status = "discovered"
    - Trigger evaluator GitHub Actions workflow (evaluator.yml)
- If track NOT allowed:
    - Still store rawData
    - Mark scrapeStatus = "success", scrapeScheduled = False, scrapeLastAttemptIso = now
    - trackAllowed = False, status = "not_supported"
    - Do NOT trigger evaluator

Callable via:
    python src/scraper.py <network> <proposal_id>
"""

import os
import json
import datetime
from typing import Dict, Any, Optional

import httpx
import firebase_admin
from firebase_admin import credentials as fb_credentials
from firebase_admin import firestore as admin_firestore

from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect.tasks import exponential_backoff
from prefect.server.schemas.states import Completed, Failed

from utils.constants import NETWORK_MAP, ALLOWED_TRACK_IDS
from utils.proposal_augmentation import generate_content_for_magis


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class ProposalFetchError(Exception):
    pass


class ProposalParseError(Exception):
    pass


class FirestoreError(Exception):
    pass


# ---------------------------------------------------------------------------
# Firebase / Firestore helpers
# ---------------------------------------------------------------------------

@task(name="Load Firestore Credentials", retries=1)
async def load_firestore_credentials() -> Dict[str, Any]:
    """
    Load Firestore service account JSON from Prefect Secret.
    Handles cases where the secret is stored as:
    - raw dict (Value type = JSON)
    - raw JSON string
    - triple-quoted JSON string
    """
    logger = get_run_logger()

    try:
        block = await Secret.load("firebase-credentials-json")
        raw = block.get()   # could be dict OR string
        logger.info(f"Loaded raw firebase credentials type: {type(raw)}")

        # Case 1: Prefect stored JSON directly as dict
        if isinstance(raw, dict):
            return raw

        # Case 2: Prefect stored JSON as string
        if isinstance(raw, str):
            cleaned = raw.strip()

            # Remove accidental triple quotes
            if cleaned.startswith('"""') and cleaned.endswith('"""'):
                cleaned = cleaned[3:-3].strip()

            # Remove accidental single quotes
            if cleaned.startswith('"') and cleaned.endswith('"'):
                cleaned = cleaned[1:-1].strip()

            return json.loads(cleaned)

        raise FirestoreError("Invalid firebase credentials format. Expected dict or JSON string.")

    except Exception as e:
        logger.error(f"Failed to load firebase credentials from Prefect Secret: {e}")
        raise FirestoreError("Failed to load firebase credentials") from e


def initialize_firebase_app_if_needed(creds_dict: Dict[str, Any]):
    """
    Initialize firebase_admin app with the provided service account dictionary.
    This is idempotent (safe to call multiple times).
    """
    if not firebase_admin._apps:
        cred = fb_credentials.Certificate(creds_dict)
        firebase_admin.initialize_app(cred)


def get_firestore_client():
    """
    Return a Firestore client via firebase_admin SDK.
    (Assumes firebase_admin.initialize_app() already called.)
    """
    return admin_firestore.client()


# ---------------------------------------------------------------------------
# Polkassembly fetch tasks
# ---------------------------------------------------------------------------

def _fetch_json(url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    with httpx.Client(timeout=30) as client:
        resp = client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()


@task(
    name="Fetch Proposal JSON from Polkassembly ReferendumV2",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    retry_jitter_factor=0.2,
)
def fetch_polkassembly_proposal_data(network: str, proposal_id: int) -> Dict[str, Any]:
    """
    Fetches and parses proposal data from Polkassembly ReferendumV2 API endpoint.

    Endpoint format (determined from NETWORK_MAP entries):
        GET <base_url>/<proposal_id>
    where NETWORK_MAP[network] should point to the base ReferendumV2 path, e.g.
        https://paseo.polkassembly.io/api/v2/ReferendumV2
    """
    logger = get_run_logger()

    if network not in NETWORK_MAP:
        msg = f"Invalid network '{network}' - not present in NETWORK_MAP"
        logger.error(msg)
        raise ProposalFetchError(msg)

    base_url = NETWORK_MAP[network].rstrip("/")
    proposal_url = f"{base_url}/{proposal_id}"

    # retrieve user agent from Prefect Secret (synchronous here, task context)
    try:
        ua_block = Secret.load("cybergov-scraper-user-agent")
        user_agent = ua_block.get()
    except Exception:
        user_agent = "cybergov-scraper/1.0"

    headers = {"User-Agent": user_agent, "Accept": "application/json"}

    logger.info(f"Fetching Polkassembly data from: {proposal_url}")

    try:
        data = _fetch_json(proposal_url, headers)
        logger.info(f"Successfully fetched JSON for {network}/{proposal_id}")
        return data
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching {proposal_url}: {e} - status {getattr(e, 'response', None)}")
        raise ProposalFetchError(f"HTTP error fetching {proposal_url}") from e
    except httpx.RequestError as e:
        logger.error(f"Request error fetching {proposal_url}: {e}")
        raise ProposalFetchError(f"Request failed for {proposal_url}") from e
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON from {proposal_url}: {e}")
        raise ProposalParseError(f"Invalid JSON from {proposal_url}") from e


@task(
    name="Fetch Polkassembly on-chain metadata",
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=10),
    retry_jitter_factor=0.2,
)
def fetch_polkassembly_onchain_metadata(network: str, proposal_id: int) -> Dict[str, Any]:
    logger = get_run_logger()

    if network not in NETWORK_MAP:
        raise ProposalFetchError(f"Invalid network '{network}'")

    base_url = NETWORK_MAP[network].rstrip("/")
    url = f"{base_url}/{proposal_id}/on-chain-metadata"

    headers = {"User-Agent": "cybergov-scraper/1.0", "Accept": "application/json"}

    logger.info(f"Fetching on-chain metadata from: {url}")
    try:
        data = _fetch_json(url, headers)
        logger.info(f"Fetched on-chain metadata for {network}/{proposal_id}")
        return data
    except Exception as e:
        logger.error(f"Failed to fetch on-chain metadata: {e}")
        return {}


# ---------------------------------------------------------------------------
# Firestore write/update tasks
# ---------------------------------------------------------------------------

@task(name="Archive previous Firestore version", retries=1)
def archive_previous_firestore_version(network: str, proposal_id: int):
    """
    Archive the previous Firestore version safely by:
    - Extracting important fields (trackNumber, proposer, title)
    - Storing the full old document as a JSON string (Firestore-safe)

    IMPORTANT: This version does NOT bump `version`.
    Version is bumped only in save_raw_data_to_firestore() to avoid double increments.
    """
    logger = get_run_logger()
    try:
        db = get_firestore_client()
        doc_id = f"{network}-{proposal_id}"
        doc_ref = db.collection("proposals").document(doc_id)
        snapshot = doc_ref.get()

        if not snapshot.exists:
            logger.info(f"No existing Firestore doc to archive for {doc_id}")
            return

        current = snapshot.to_dict()

        # --- Extract important fields for querying ------------------------
        onchain_meta = current.get("OnChain_MetaData", {})
        track_number = (
            onchain_meta.get("trackNumber")
            or current.get("trackNumber")
        )
        proposer = onchain_meta.get("proposer")
        title = current.get("title")
        proposal_type = current.get("proposalType")

        # --- Convert entire document to JSON string (SAFE) ----------------
        try:
            payload_str = json.dumps(current, default=str)
        except Exception as e:
            logger.error(f"Failed JSON encode during archive: {e}")
            payload_str = "{}"

        # --- Versioning subcollection ------------------------------------
        rev_ts = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        versions_col = doc_ref.collection("versions")
        version_index = rev_ts

        versions_col.document(str(version_index)).set({
            "archivedAt": rev_ts,
            "archivedBy": "cybergov-scraper",
            "trackNumber": track_number,
            "proposer": proposer,
            "title": title,
            "proposalType": proposal_type,
            "payload": payload_str,
        })

        logger.info(f"Archived existing document into proposals/{doc_id}/versions/{version_index}")

    except Exception as e:
        logger.error(f"Failed to archive Firestore doc for {network}-{proposal_id}: {e}")
        raise FirestoreError("Failed to archive existing Firestore doc") from e


@task(name="Save Raw Data to Firestore", retries=2, retry_delay_seconds=5)
def save_raw_data_to_firestore(raw_proposal_data: Dict[str, Any], network: str, proposal_id: int):
    """
    Create (or merge) a Firestore document for this proposal with raw data.
    Document id: "{network}-{proposal_id}"

    Schema:
      - files.rawData = raw_proposal_data (includes OnChain_MetaData)
      - status = "discovered" (will later be updated to "not_supported" if track not allowed)
      - discoveredAt, createdAt, updatedAt
      - createdBy preserved if exists, otherwise "cybergov-scraper"
    """
    logger = get_run_logger()
    try:
        db = get_firestore_client()
        doc_id = f"{network}-{proposal_id}"
        doc_ref = db.collection("proposals").document(doc_id)

        now_iso = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        current_doc = doc_ref.get()

        if current_doc.exists:
            current = current_doc.to_dict()
            version = (current.get("version") or 1) + 1
            created_at = current.get("createdAt", now_iso)
            created_by = current.get("createdBy", "cybergov-detector")
        else:
            version = 1
            created_at = now_iso
            created_by = "cybergov-scraper"

        payload = {
            "network": network,
            "proposalId": int(proposal_id),
            "status": "discovered",
            "discoveredAt": now_iso,
            "createdAt": created_at,
            "updatedAt": now_iso,
            "createdBy": created_by,
            "lastModifiedBy": "cybergov-scraper",
            "version": version,
            "files": {
                "rawData": raw_proposal_data
            },
        }

        # Use merge=True so we don't blow away fields like discoveredAtIso, scrapeStatus, etc.
        doc_ref.set(payload, merge=True)
        logger.info(f"Saved raw proposal data to Firestore: proposals/{doc_id}")
    except Exception as e:
        logger.error(f"Failed to save raw data to Firestore for {network}-{proposal_id}: {e}")
        raise FirestoreError("Failed to write raw data to Firestore") from e


@task(name="Update Firestore with Generated Content", retries=2, retry_delay_seconds=5)
def update_firestore_with_content(content_md: str, network: str, proposal_id: int):
    """
    Update the proposal document to add generated content and bump updatedAt.
    """
    logger = get_run_logger()
    try:
        db = get_firestore_client()
        doc_id = f"{network}-{proposal_id}"
        doc_ref = db.collection("proposals").document(doc_id)

        now_iso = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

        update_payload = {
            "files.content": content_md,
            "updatedAt": now_iso,
            "lastModifiedBy": "cybergov-scraper",
        }

        doc_ref.update(update_payload)
        logger.info(f"Updated proposals/{doc_id} with generated content.")
    except Exception as e:
        logger.error(f"Failed to update Firestore content for {network}-{proposal_id}: {e}")
        raise FirestoreError("Failed to update Firestore with content") from e


@task(name="Update Scrape Metadata", retries=2, retry_delay_seconds=5)
def update_scrape_metadata(network: str, proposal_id: int, track_allowed: bool, status_value: str):
    """
    Mark scraping outcome in Firestore:
      - scrapeStatus: "success"
      - scrapeScheduled: False
      - scrapeLastAttemptIso: now
      - trackAllowed: True/False
      - status: "discovered" or "not_supported"
    """
    logger = get_run_logger()
    try:
        db = get_firestore_client()
        doc_id = f"{network}-{proposal_id}"
        doc_ref = db.collection("proposals").document(doc_id)

        now_iso = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

        doc_ref.update({
            "trackAllowed": track_allowed,
            "status": status_value,
            "scrapeStatus": "success",
            "scrapeScheduled": False,
            "scrapeLastAttemptIso": now_iso,
            "updatedAt": now_iso,
            "lastModifiedBy": "cybergov-scraper",
        })
        logger.info(f"Updated scrape metadata for proposals/{doc_id}")
    except Exception as e:
        logger.error(f"Failed to update scrape metadata for {network}-{proposal_id}: {e}")
        raise FirestoreError("Failed to update scrape metadata") from e


# ---------------------------------------------------------------------------
# Enrichment and MAGIS content generation
# ---------------------------------------------------------------------------

@task(name="Enrich proposal data (placeholder)")
def enrich_proposal_data(raw_proposal_data: Dict[str, Any], network: str, proposal_id: int):
    """
    Placeholder: enrich the raw proposal data with additional on-chain metadata.
    This function receives the raw data (dict) and should return an updated dict if needed.
    For now it returns the input unchanged.
    """
    logger = get_run_logger()
    logger.info(f"Enriching proposal {network}/{proposal_id} (placeholder)")
    return raw_proposal_data


@task(name="Generate Prompt Content for LLM")
def generate_prompt_content(raw_proposal_data: Dict[str, Any], network: str, proposal_id: int) -> str:
    """
    Generate the markdown content for MAGIS based on the raw proposal data.
    Returns markdown string content.

    Fixes:
      - Uses correct parameter name: proposal_data=...
      - Injects proposalId into proposal_data before generation
      - Lets generate_content_for_magis() read API key from env (GEMINI/OPENROUTER) internally
    """
    logger = get_run_logger()
    raw_proposal_data["proposalId"] = proposal_id
    logger.info(f"Generating content for {network} proposal {raw_proposal_data.get('proposalId')}")

    # Do NOT try to load a Prefect Secret here; let utils read from env (GEMINI_API_KEY / OPENROUTER_API_KEY)
    content_md = generate_content_for_magis(
        proposal_data=raw_proposal_data,
        logger=logger,
        openrouter_model="openrouter/anthropic/claude-sonnet-4",
        openrouter_api_key=None,  # utils will consult environment themselves
        network=network,
    )

    logger.info("Content generation completed.")
    return content_md


# ---------------------------------------------------------------------------
# Validation helpers (track)
# ---------------------------------------------------------------------------

def extract_track_value(proposal_data: Dict[str, Any]) -> Optional[int]:
    # direct keys
    for key in ("trackNumber", "track_number", "track"):
        val = proposal_data.get(key)
        if val is not None:
            try:
                return int(val)
            except Exception:
                pass

    # onchainData may be dict or list with nested track values
    onchain = proposal_data.get("onchainData") or {}
    if isinstance(onchain, list) and onchain:
        onchain = onchain[0]
    for key in ("trackNumber", "track_number", "track"):
        val = onchain.get(key) if isinstance(onchain, dict) else None
        if val is not None:
            try:
                return int(val)
            except Exception:
                pass

    # last-resort: nested keys like payloads returned by on-chain endpoint
    nested = proposal_data.get("OnChain_MetaData") or proposal_data.get("onchainMetadata") or {}
    for key in ("trackNumber", "track_number", "track"):
        val = nested.get(key)
        if val is not None:
            try:
                return int(val)
            except Exception:
                pass

    return None


def validate_proposal_track_polkassembly(proposal_data: Dict[str, Any]) -> bool:
    """
    Validate that the proposal track (extracted) is in the allowed list.
    """
    logger = get_run_logger()
    track = extract_track_value(proposal_data)
    if track is None:
        logger.warning("Could not extract track id from proposal_data.")
        return False
    if track not in ALLOWED_TRACK_IDS:
        logger.warning(f"Proposal track {track} not in allowed tracks {ALLOWED_TRACK_IDS}")
        return False
    logger.info(f"✅ Proposal track {track} is valid")
    return True


# ---------------------------------------------------------------------------
# Trigger Evaluator GitHub Actions
# ---------------------------------------------------------------------------

def trigger_evaluator_github_workflow(network: str, proposal_id: int):
    """
    Trigger evaluator GitHub Actions workflow:
      - Workflow file: .github/workflows/evaluator.yml
      - Uses GITHUB_REPOSITORY (owner/repo) and GITHUB_TOKEN

    This replaces the old Prefect 'schedule_inference_task' behavior.
    """
    logger = get_run_logger()

    repo = os.getenv("GITHUB_REPOSITORY")
    token = os.getenv("GITHUB_TOKEN")
    # If running locally, skip evaluator trigger
    if not repo or not token:
        print("[SCRAPER] Local mode detected → evaluator trigger skipped")
        return False

    if not repo:
        raise RuntimeError("GITHUB_REPOSITORY not set (expected 'owner/repo').")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set (GitHub PAT or Actions token).")

    url = f"https://api.github.com/repos/{repo}/actions/workflows/evaluator.yml/dispatches"
    payload = {
        "ref": "main",
        "inputs": {
            "network": network,
            "proposal_id": str(proposal_id),
        },
    }

    logger.info(f"Triggering evaluator workflow at {url} for {network}/{proposal_id}")
    resp = httpx.post(
        url,
        json=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        },
        timeout=20,
    )

    if resp.status_code not in (200, 201, 202, 204):
        raise RuntimeError(f"Failed to trigger evaluator workflow: {resp.status_code} {resp.text}")

    logger.info("✅ Evaluator workflow triggered successfully.")


# ---------------------------------------------------------------------------
# Main flow
# ---------------------------------------------------------------------------

@flow(name="Fetch Proposal Data (Polkassembly to Firestore + Trigger Evaluator)")
async def fetch_proposal_data(
    network: str,
    proposal_id: int,
):
    """
    Fetch relevant data for a ReferendumV2 proposal from Polkassembly,
    store raw data in Firestore, generate MAGIS prompt content if track-allowed,
    update scrape metadata, and trigger evaluator GitHub workflow for allowed tracks.
    """
    logger = get_run_logger()

    if network not in NETWORK_MAP:
        logger.error(
            f"Invalid network '{network}'. Must be one of {list(NETWORK_MAP.keys())}"
        )
        return Failed(message=f"Invalid network: {network}")

    try:
        # 1) Load credentials & initialize Firebase
        logger.info("Loading Firestore credentials and initializing Firebase...")
        creds = await load_firestore_credentials()
        initialize_firebase_app_if_needed(creds)

        # 2) Archive previous Firestore doc (if exists)
        logger.info("Archiving previous Firestore doc if exists...")
        archive_previous_firestore_version(network=network, proposal_id=proposal_id)

        # 3) Fetch raw data + on-chain metadata from Polkassembly
        logger.info(f"Fetching data for proposal {proposal_id} on {network}")
        raw_proposal_data = fetch_polkassembly_proposal_data(network=network, proposal_id=proposal_id)
        onchain_meta = fetch_polkassembly_onchain_metadata(network=network, proposal_id=proposal_id)

        # Merge trackNumber into proposal data & store OnChain_MetaData exactly as before
        if "trackNumber" in onchain_meta:
            raw_proposal_data["trackNumber"] = onchain_meta["trackNumber"]

        raw_proposal_data["OnChain_MetaData"] = onchain_meta

        # 4) Save raw data to Firestore (files.rawData = raw_proposal_data)
        save_raw_data_to_firestore(raw_proposal_data=raw_proposal_data, network=network, proposal_id=proposal_id)

        # 5) Validate track
        logger.info("Validating proposal track...")
        track_ok = validate_proposal_track_polkassembly(raw_proposal_data)

        if not track_ok:
            # Track not allowed: still mark scrape success but status = "not_supported"
            message = "Track not delegated to CyberGov. Marked as not_supported, no evaluator triggered."
            logger.warning(message)
            update_scrape_metadata(
                network=network,
                proposal_id=proposal_id,
                track_allowed=False,
                status_value="not_supported",
            )
            return Completed(message=message)

        # 6) Enrich data (placeholder)
        enriched_data = enrich_proposal_data(
            raw_proposal_data=raw_proposal_data,
            network=network,
            proposal_id=proposal_id,
        )

        # 7) Generate prompt content (in-memory, uses correct proposal_data param)
        content_md = generate_prompt_content(
            raw_proposal_data=enriched_data,
            network=network,
            proposal_id=proposal_id,
        )

        # 8) Update Firestore with generated content
        update_firestore_with_content(
            content_md=content_md,
            network=network,
            proposal_id=proposal_id,
        )

        # 9) Mark scrape as success + trackAllowed = True
        update_scrape_metadata(
            network=network,
            proposal_id=proposal_id,
            track_allowed=True,
            status_value="discovered",
        )

        # 10) Trigger evaluator via GitHub Actions
        logger.info("All good! Triggering evaluator workflow...")
        trigger_evaluator_github_workflow(network=network, proposal_id=proposal_id)

        return Completed(message=f"Successfully processed {network}/{proposal_id}")

    except (ProposalFetchError, ProposalParseError, FirestoreError) as e:
        message = f"Failed processing {network} proposal {proposal_id}: {e}"
        logger.error(message, exc_info=True)
        # Optional: here you could mark scrapeStatus="failed" in Firestore if doc exists
        return Failed(message=message)
    except Exception as e:
        message = f"Unexpected error processing {network} proposal {proposal_id}: {e}"
        logger.error(message, exc_info=True)
        return Failed(message=message)


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    import asyncio

    if len(sys.argv) != 3:
        print("Usage: python src/scraper.py <network> <proposal_id>")
        sys.exit(1)

    network_arg = sys.argv[1]
    proposal_id_arg = int(sys.argv[2])

    asyncio.run(
        fetch_proposal_data(
            network=network_arg,
            proposal_id=proposal_id_arg,
        )
    )
