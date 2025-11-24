
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))

import json
import logging
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests
from fastapi import (
    FastAPI,
    HTTPException,
    Depends,
    Security,
    Header,
    status,
)
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from dotenv import load_dotenv

# ---------------------------------------------------
# ENV + PATHS
# ---------------------------------------------------

load_dotenv()

# Make sure we can import src/utils/*
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(BASE_DIR, "..", "src")
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.firestore_helper import FirestoreHelper
from utils.comment_generator import (
    extract_agent_reasons,
    generate_comment,
    format_delegatex_payload,
)
from utils.weighted_decision_engine import list_available_templates
from utils.constants import GOV_BOT_VERSION

# ---------------------------------------------------
# LOGGING
# ---------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("GovBotAPI")

# ---------------------------------------------------
# FASTAPI APP
# ---------------------------------------------------

app = FastAPI(
    title="Gov Bot API Service",
    description="Gov Bot brain → user bot management + DelegateX bridge.",
    version="1.0.0",
)

# ---------------------------------------------------
# SECURITY HEADERS (x-passcode, x-network)
# ---------------------------------------------------

API_KEY_NAME = "x-passcode"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


def get_passcode(api_key_header: str = Security(api_key_header)):
    """
    Validates the x-passcode header against DELEGATE_X_SECRET env var.
    """
    expected_key = os.getenv("DELEGATE_X_SECRET")

    if not expected_key:
        logger.error("DELEGATE_X_SECRET not set in environment! Blocking all requests.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server misconfiguration: API Key not set.",
        )

    if api_key_header == expected_key:
        return api_key_header

    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Invalid passcode",
    )


def get_network(x_network: Optional[str] = Header(None)) -> str:
    """
    Extract the network from header. Defaults to 'polkadot'.
    """
    return (x_network or "polkadot").lower()


# ---------------------------------------------------
# FIRESTORE HELPER INIT
# ---------------------------------------------------

try:
    db_helper = FirestoreHelper()
    logger.info("FirestoreHelper initialized successfully.")
except Exception as e:
    logger.error(f"Failed to initialize FirestoreHelper: {e}")
    db_helper = None

# ---------------------------------------------------
# Pydantic MODELS
# ---------------------------------------------------


class CreateBotRequest(BaseModel):
    user_id: str
    strategy_id: str = "neutral"
    signature_link: Optional[str] = None
    contact_link: Optional[str] = None
    wallet_address: Optional[str] = None
    preferences: Optional[Dict[str, Any]] = None


class CreateBotResponse(BaseModel):
    status: str
    message: str
    user_id: str
    strategy_id: str
    batch_vote_triggered: bool


class SendVoteRequest(BaseModel):
    user_id: str
    proposal_id: str  # plain ID like "123"
    # network from header; body override allowed but optional
    network: Optional[str] = None


class SendVoteResponse(BaseModel):
    status: str
    message: str
    delegatex_status_code: int
    delegatex_response: Any
    payload_sent: Dict[str, Any]


# ---------------------------------------------------
# HELPER: Trigger batch-vote workflow (optional)
# ---------------------------------------------------


def trigger_batch_vote_for_user(user_id: str, network: str) -> bool:
    """
    Optional: trigger a GitHub Actions workflow to run batch voting
    for this user across all active proposals.

    Controlled by envs:
      BATCH_VOTE_REPO         (e.g. "Eishaan-Khatri/cybergov")
      BATCH_VOTE_WORKFLOW     (e.g. "batch_vote.yml")
      GITHUB_TOKEN            (PAT or repo token)

    Returns True if dispatch accepted, False otherwise.
    """
    repo = os.getenv("BATCH_VOTE_REPO")
    workflow = os.getenv("BATCH_VOTE_WORKFLOW")
    token = os.getenv("GITHUB_TOKEN")

    if not (repo and workflow and token):
        logger.warning(
            "Batch vote trigger skipped: BATCH_VOTE_REPO / BATCH_VOTE_WORKFLOW / GITHUB_TOKEN not fully set."
        )
        return False

    url = (
        f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches"
    )

    body = {
        "ref": "main",
        "inputs": {
            "user_id": user_id,
            "network": network,
        },
    }

    try:
        resp = requests.post(
            url,
            json=body,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            },
            timeout=20,
        )
        if resp.status_code in (200, 201, 204):
            logger.info(
                f"Batch-vote workflow triggered for user={user_id}, network={network}"
            )
            return True

        logger.error(
            f"Failed to trigger batch vote workflow: {resp.status_code} {resp.text}"
        )
        return False

    except Exception as e:
        logger.error(f"Exception while triggering batch vote workflow: {e}")
        return False


# ---------------------------------------------------
# HELPER: Push vote to Delegate-X
# ---------------------------------------------------


def push_vote_to_delegatex(
    network: str,
    user_id: str,
    proposal_id: str,
    vote_doc: Dict[str, Any],
    proposal_doc: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build payload from stored vote + proposal data, send to Delegate-X API,
    and return {status_code, response_json_or_text, payload}.
    """

    # 1) Extract vote_data from vote.vote.content JSON
    vote_block = vote_doc.get("vote", {})
    content_raw = vote_block.get("content")

    if not content_raw:
        raise HTTPException(
            status_code=400,
            detail="vote.content missing in user vote document.",
        )

    if isinstance(content_raw, dict):
        vote_data = content_raw
    else:
        try:
            vote_data = json.loads(content_raw)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse vote.content as JSON: {e}")
            raise HTTPException(
                status_code=500,
                detail="Stored vote.content is not valid JSON.",
            )

    # 2) Extract agent reasons from proposal
    agent_reasons = extract_agent_reasons(proposal_doc)

    # 3) Comment: prefer stored comment, else generate from Gemini
    comment = vote_doc.get("comment")
    if not comment:
        try:
            comment = generate_comment(vote_data, agent_reasons)
        except Exception as e:
            logger.warning(f"generate_comment failed, using fallback: {e}")
            decision_reasoning = vote_data.get("weighted_decision_metadata", {}).get(
                "decision_reasoning", ""
            )
            final_decision = vote_data.get("final_decision", "Abstain")
            strategy_used = vote_data.get(
                "weighted_decision_metadata", {}
            ).get("strategy_used", "neutral")
            comment = (
                f"CyberGov Bot voted {final_decision.upper()} "
                f"using {strategy_used} strategy. {decision_reasoning}"
            )

    # 4) Use helper to format DelegateX payload
    payload = format_delegatex_payload(
        user_id=user_id,
        proposal_id=proposal_id,
        vote_data=vote_data,
        agent_reasons=agent_reasons,
        comment=comment,
    )

    # 5) Send POST to Delegate-X endpoint
    base_url = os.getenv(
        "DELEGATEX_BASE_URL",
        "https://polkassembly-v2-git-feat-cyphergovbot-fevm-polkassembly-next.vercel.app/api/v2/delegate-x/vote-delegate-x",
    )

    outbound_passcode = os.getenv("DELEGATEX_OUTBOUND_PASSCODE") or os.getenv(
        "DELEGATE_X_SECRET"
    )
    if not outbound_passcode:
        raise HTTPException(
            status_code=500,
            detail="DELEGATEX_OUTBOUND_PASSCODE or DELEGATE_X_SECRET not set.",
        )

    headers = {
        "x-passcode": outbound_passcode,
        "x-network": network,
        "Content-Type": "application/json",
    }

    logger.info(f"Sending vote to DelegateX at {base_url} for {network}-{proposal_id}")

    try:
        resp = requests.post(base_url, json=payload, headers=headers, timeout=25)
    except Exception as e:
        logger.error(f"Error while calling DelegateX: {e}")
        raise HTTPException(
            status_code=502,
            detail=f"Failed to reach DelegateX: {e}",
        )

    # Try to parse JSON response
    try:
        resp_body = resp.json()
    except Exception:
        resp_body = resp.text

    return {
        "status_code": resp.status_code,
        "response": resp_body,
        "payload": payload,
    }


# ---------------------------------------------------
# ROUTES
# ---------------------------------------------------


@app.get("/")
def health_check():
    return {
        "status": "ok",
        "service": "GovBot API",
        "version": GOV_BOT_VERSION,
    }


@app.get("/strategies", dependencies=[Depends(get_passcode)])
def get_strategies():
    """
    List available strategy templates for UI.
    """
    templates = list_available_templates()
    return {"strategies": templates, "count": len(templates)}


@app.post(
    "/create-bot",
    response_model=CreateBotResponse,
    dependencies=[Depends(get_passcode)],
)
def create_user_bot(
    request: CreateBotRequest,
    network: str = Depends(get_network),
):
    """
    Create or update a user bot configuration.

    Body:
      - user_id
      - strategy_id
      - signature_link? (optional)
      - contact_link? (optional)
      - wallet_address? (optional)
      - preferences? (optional JSON)

    Side effects:
      - Writes/merges users/{userId} document
      - Optionally triggers batch-vote workflow so all active proposals
        get a vote for this user.
    """
    if not db_helper:
        raise HTTPException(
            status_code=500,
            detail="Database connection not available",
        )

    try:
        # Validate strategy
        strategies = list_available_templates()
        if request.strategy_id not in strategies:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid strategy: {request.strategy_id}",
            )

        db_helper.create_user(
            user_id=request.user_id,
            strategy_id=request.strategy_id,
            wallet_address=request.wallet_address,
            signature_link=request.signature_link,
            contact_link=request.contact_link,
            preferences=request.preferences,
        )

        # Optional: trigger batch vote workflow for this user
        batch_triggered = trigger_batch_vote_for_user(
            user_id=request.user_id,
            network=network,
        )

        return CreateBotResponse(
            status="success",
            message=(
                f"Bot configured for user {request.user_id} "
                f"with strategy {request.strategy_id}"
            ),
            user_id=request.user_id,
            strategy_id=request.strategy_id,
            batch_vote_triggered=batch_triggered,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in /create-bot: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.post(
    "/send-vote-delegate-x",
    response_model=SendVoteResponse,
    dependencies=[Depends(get_passcode)],
)
def send_vote_delegatex(
    request: SendVoteRequest,
    network: str = Depends(get_network),
):
    """
    INTERNAL BRIDGE:

    Reads stored user vote + agent reasons and pushes a single vote
    to the DelegateX /vote-delegate-x endpoint.

    Body:
      {
        "user_id": "...",
        "proposal_id": "...",  # plain id, e.g. "123"
        "network": "polkadot"  # optional override
      }

    Steps:
      - Load users/{userId}/votes/{network}-{proposalId}
      - Load proposals/{network}-{proposalId}
      - Use vote.vote.content JSON as vote_data
      - Use extract_agent_reasons(proposal_doc)
      - Use stored comment (or generate_comment fallback)
      - Calls DelegateX with proper body
      - Saves DelegateX response back into user vote doc
    """
    if not db_helper:
        raise HTTPException(
            status_code=500,
            detail="Database connection not available",
        )

    target_network = (request.network or network).lower()
    user_id = request.user_id
    proposal_id = request.proposal_id
    vote_doc_id = f"{target_network}-{proposal_id}"

    try:
        # 1) Fetch user vote doc
        user_ref = db_helper.db.collection("users").document(user_id)
        vote_ref = user_ref.collection("votes").document(vote_doc_id)
        vote_snap = vote_ref.get()

        if not vote_snap.exists:
            raise HTTPException(
                status_code=404,
                detail=f"Vote doc not found for user={user_id}, proposal={vote_doc_id}.",
            )

        vote_doc = vote_snap.to_dict()

        # 2) Fetch proposal doc for agent reasons
        proposal_ref = db_helper.db.collection("proposals").document(vote_doc_id)
        proposal_snap = proposal_ref.get()
        if not proposal_snap.exists:
            raise HTTPException(
                status_code=404,
                detail=f"Proposal doc {vote_doc_id} not found.",
            )

        proposal_doc = proposal_snap.to_dict()

        # 3) Build payload + call DelegateX
        dx_result = push_vote_to_delegatex(
            network=target_network,
            user_id=user_id,
            proposal_id=proposal_id,
            vote_doc=vote_doc,
            proposal_doc=proposal_doc,
        )

        # 4) Save DelegateX response back to Firestore
        try:
            vote_ref.update(
                {
                    "delegateX": {
                        "status_code": dx_result["status_code"],
                        "response": dx_result["response"],
                        "sentAt": datetime.now(timezone.utc).isoformat(),
                    },
                    "updatedAt": datetime.now(timezone.utc).isoformat(),
                }
            )
        except Exception as e:
            logger.error(f"Failed to write delegateX response to Firestore: {e}")

        return SendVoteResponse(
            status="success"
                if 200 <= dx_result["status_code"] < 300
                else "error",
            message="Vote sent to DelegateX",
            delegatex_status_code=dx_result["status_code"],
            delegatex_response=dx_result["response"],
            payload_sent=dx_result["payload"],
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(
            f"Error in /send-vote-delegate-x for {user_id} {vote_doc_id}: {e}",
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------
# LOCAL DEV ENTRYPOINT
# ---------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
