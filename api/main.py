import os
import json
import requests
from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from utils.firestore_helper import FirestoreHelper
from utils.weighted_decision_engine import compute_weighted_decision
from utils.comment_generator import generate_comment, extract_agent_reasons

load_dotenv()

app = FastAPI(title="GovBot API", version="1.0.0")

# -------------------------------
# Security
# -------------------------------

def require_passcode(x_passcode: str = Header(...)):
    expected = os.getenv("DELEGATE_X_SECRET")
    if not expected:
        raise HTTPException(500, "Server error: DELEGATE_X_SECRET missing")
    if x_passcode != expected:
        raise HTTPException(403, "Invalid x-passcode")
    return True

def require_network(x_network: str = Header(default="polkadot")):
    return x_network.lower()


# -------------------------------
# Firestore
# -------------------------------
db = FirestoreHelper()


# -------------------------------
# MODELS
# -------------------------------

class CreateBotRequest(BaseModel):
    user_id: str
    strategy_id: str
    signature_link: str | None = None
    contact_link: str | None = None
    preferences: dict | None = None


class VoteRequest(BaseModel):
    user_id: str = Field(..., min_length=1)
    proposal_id: str = Field(..., min_length=1)


# -------------------------------
# HEALTH
# -------------------------------

@app.get("/")
def root():
    return {"status": "ok", "service": "govbot"}


# -------------------------------
# CREATE USER BOT
# -------------------------------

@app.post("/create-bot")
def create_bot(
    body: CreateBotRequest,
    _auth=Depends(require_passcode),
    network=Depends(require_network)
):
    """
    Called ONCE by Delegate-X.
    """
    try:
        db.create_user(
            user_id=body.user_id,
            strategy_id=body.strategy_id,
            wallet_address=None,
            signature_link=body.signature_link,
            contact_link=body.contact_link,
            preferences=body.preferences
        )
        return {"status": "success", "user_id": body.user_id}
    except Exception as e:
        raise HTTPException(500, str(e))


# -------------------------------
# VOTE DELEGATE-X
# -------------------------------

@app.post("/vote-delegate-x")
def vote_delegate_x(
    req: VoteRequest,
    _auth=Depends(require_passcode),
    network=Depends(require_network)
):
    """
    This endpoint:
    - Loads user strategy
    - Loads 3-agent proposal outputs from Firestore
    - Computes weighted vote
    - Generates comment
    - Sends POST to Delegate-X backend
    """
    # -------------------------------
    # 1. Load User
    # -------------------------------
    user_doc = db.db.collection("users").document(req.user_id).get()
    if not user_doc.exists:
        raise HTTPException(404, "User not found. Use /create-bot first.")
    user = user_doc.to_dict()
    strategy_id = user.get("strategyId")

    # -------------------------------
    # 2. Load Proposal Evaluations
    # -------------------------------
    fpid = f"{network}-{req.proposal_id}"
    proposal_doc = db.db.collection("proposals").document(fpid).get()
    if not proposal_doc.exists:
        raise HTTPException(404, "Proposal not scraped/evaluated yet")
    proposal_data = proposal_doc.to_dict()

    # Extract reasons
    agent_reasons = extract_agent_reasons(proposal_data)
    agent_votes = []
    files = proposal_data.get("files", {}).get("outputs", {})

    for agent in ["balthazar", "caspar", "melchior"]:
        if agent not in files:
            continue
        data = files[agent].get("content", "{}")
        if isinstance(data, str):
            data = json.loads(data)
        agent_votes.append({
            "agent": agent,
            "decision": data.get("decision", "Abstain")
        })

    if not agent_votes:
        raise HTTPException(400, "Missing agent evaluations")

    # -------------------------------
    # 3. Weighted decision
    # -------------------------------
    result = compute_weighted_decision(agent_votes, template_name=strategy_id)

    # -------------------------------
    # 4. Comment
    # -------------------------------
    vote_data = {
        "final_decision": result.final_decision,
        "weighted_decision_metadata": {
            "strategy_used": result.template_used,
            "decision_reasoning": result.decision_reasoning,
        }
    }
    comment = generate_comment(vote_data, agent_reasons)

    # -------------------------------
    # 5. Map decision
    # -------------------------------
    decision_map = {"Nay": 0, "Aye": 1, "Abstain": 2}
    decision_int = decision_map.get(result.final_decision, 2)

    # -------------------------------
    # 6. POST to Delegate-X
    # -------------------------------
    url = (
        "https://polkassembly-v2-git-feat-cyphergovbot-fevm-"
        "polkassembly-next.vercel.app/api/v2/delegate-x/vote-delegate-x"
    )

    payload = {
        "userId": req.user_id,
        "proposalId": req.proposal_id,
        "decision": decision_int,
        "reason": {
            "casper": agent_reasons.get("casper", []),
            "second": agent_reasons.get("second", []),
            "third": agent_reasons.get("third", []),
        },
        "comment": comment
    }

    headers = {
        "x-passcode": os.getenv("DELEGATE_X_SECRET"),
        "x-network": network,
        "Content-Type": "application/json",
    }

    r = requests.post(url, json=payload, headers=headers)
    ok = r.status_code in (200, 201, 204)

    if not ok:
        raise HTTPException(500, f"Delegate-X rejected: {r.text}")

    # -------------------------------
    # 7. Return to caller
    # -------------------------------
    return {
        "status": "sent",
        "decision": decision_int,
        "comment": comment,
        "reason": agent_reasons
    }
