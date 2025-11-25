# src/batch_vote.py

import os
import json
import requests
import datetime
import argparse

from utils.firestore_helper import FirestoreHelper
from utils.weighted_decision_engine import compute_weighted_decision

# ---------------------------------------------
# Firestore
# ---------------------------------------------
def init_firestore():
    try:
        return FirestoreHelper().db
    except Exception as e:
        raise RuntimeError(f"Firestore initialization failed: {e}")

db = init_firestore()

# ---------------------------------------------
# Load agent file content from Firestore
# Reading from:
# proposals/{network-proposal}/files/outputs/{agent}
# ---------------------------------------------
def load_agent_file(db, network, proposal_id, agent):
    """
    Load agent file content from Firestore
    Reading from: proposals/{network-proposal}/files/outputs/{agent}
    """
    doc_ref = (
        db.collection("proposals")
        .document(f"{network}-{proposal_id}")
    )
    
    doc = doc_ref.get()
    
    if not doc.exists:
        print(f"   ❌ Document proposals/{network}-{proposal_id} does not exist")
        return None
    
    doc_data = doc.to_dict()
    
    # Navigate to files -> outputs -> agent
    files = doc_data.get("files", {})
    if not files:
        print(f"   ⚠ No 'files' field in document")
        return None
    
    outputs = files.get("outputs", {})
    if not outputs:
        print(f"   ⚠ No 'outputs' field in files")
        return None
    
    agent_data = outputs.get(agent)
    if not agent_data:
        print(f"   ⚠ Agent '{agent}' not found in outputs")
        return None
    
    raw = agent_data.get("content")
    if not raw:
        print(f"   ⚠ No 'content' field for agent '{agent}'")
        return None
    
    try:
        parsed = json.loads(raw)
        return parsed
    except Exception as e:
        print(f"   ❌ JSON parse error for '{agent}': {e}")
        return None
# ---------------------------------------------
# First line of rationale
# ---------------------------------------------
def extract_first_sentence(rationale: str):
    if not rationale or not isinstance(rationale, str):
        return None
    return rationale.split(".")[0].strip()


# ---------------------------------------------
# Convert to DelegateX numeric code
# ---------------------------------------------
def decision_to_code(dec):
    d = dec.lower()
    if d == "aye":
        return 1
    if d == "nay":
        return 0
    return 2


# ---------------------------------------------
# Batch Vote Execution
# ---------------------------------------------
def run_batch_vote(network: str, proposal_id: str):
    proposal_key = f"{network}-{proposal_id}"

    print(f"\n🔥 Running batch-vote for proposal {proposal_key}\n")

    users = db.collection("users").stream()

    for user in users:
        user_data = user.to_dict()

        delegatex_user_id = user_data.get("delegatexUserId")
        strategy_id = user_data.get("strategyId", "neutral")

        if not delegatex_user_id:
            print(f"⚠ Skipping user {user.id}: no delegatexUserId")
            continue

        # ------------------------------------------------
        # STEP 1 — Load 3 agent outputs
        # ------------------------------------------------
        A_balthazar = load_agent_file(db, network, proposal_id, "balthazar")
        A_caspar    = load_agent_file(db, network, proposal_id, "caspar")
        A_melchior  = load_agent_file(db, network, proposal_id, "melchior")

        if not A_balthazar and not A_caspar and not A_melchior:
            print(f"⚠ No agent outputs found for proposal {proposal_key}")
            continue

        # Build agent_votes list
        agent_votes = []
        for agent_name, blob in [
            ("balthazar", A_balthazar),
            ("caspar", A_caspar),
            ("melchior", A_melchior),
        ]:
            if blob:
                agent_votes.append({
                    "agent": agent_name,
                    "decision": blob.get("decision", "Abstain"),
                })

        if not agent_votes:
            print(f"⚠ No valid agent votes for user {delegatex_user_id}")
            continue

        # ------------------------------------------------
        # STEP 2 — Weighted final decision (user strategy)
        # ------------------------------------------------
        weighted = compute_weighted_decision(
            agent_votes=agent_votes,
            template_name=strategy_id
        )

        final_decision = weighted.final_decision  # "Aye" | "Nay" | "Abstain"
        dx_decision = decision_to_code(final_decision)

        # ------------------------------------------------
        # STEP 3 — Extract first-line rationales for DelegateX
        # ------------------------------------------------
        reasons = []

        for blob in [A_balthazar, A_caspar, A_melchior]:
            if blob and blob.get("rationale"):
                first = extract_first_sentence(blob["rationale"])
                if first:
                    reasons.append(first)

        if not reasons:
            reasons = ["Automated rationale unavailable."]

        # ------------------------------------------------
        # STEP 4 — SAVE vote in Firestore BEFORE sending
        # ------------------------------------------------
        user_vote_ref = (
            db.collection("users")
            .document(str(delegatex_user_id))
            .collection("votes")
            .document(proposal_key)
        )

        vote_content = {
            "timestamp_utc": datetime.datetime.utcnow().isoformat(),
            "final_decision": final_decision,
            "agent_votes": weighted.agent_votes,
            "weighted_scores": weighted.weighted_scores,
            "strategy_used": weighted.template_used,
            "template_weights": weighted.template_weights,
            "decision_reasoning": weighted.decision_reasoning,
            "rules_triggered": weighted.rules_triggered,
            "comment": f"Automated CyberGov vote: {final_decision.upper()}",
        }

        user_vote_ref.set({
            "delegatexUserId": delegatex_user_id,
            "proposal_id": proposal_id,
            "vote": {
                "content": json.dumps(vote_content),
                "timestamp_utc": datetime.datetime.utcnow().isoformat(),
            },
            "savedAt": datetime.datetime.utcnow().isoformat()
        }, merge=True)

        # ------------------------------------------------
        # STEP 5 — Prepare DelegateX API call
        # ------------------------------------------------
        payload = {
            "userId": str(delegatex_user_id),
            "proposalId": str(proposal_id),
            "decision": dx_decision,
            "reason": reasons,
            "comment": f"Automated CyberGov vote: {final_decision.upper()}",
        }

        url = (
            "https://polkassembly-v2-git-feat-cyphergovbot-fevm-polkassembly-next.vercel.app"
            "/api/v2/delegate-x/vote-delegate-x"
        )

        headers = {
            "x-passcode": os.getenv("DELEGATE_X_SECRET"),
            "x-network": network,
            "Content-Type": "application/json",
        }

        print(f"➡ Sending vote for user {delegatex_user_id}")
        print("   Payload:", json.dumps(payload, indent=2))

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=20)
            resp_body = resp.text
            print("   ⇦ Raw response:", resp_body)
            print(f"   DelegateX status: {resp.status_code}")

            # Save DelegateX response
            user_vote_ref.update({
                "delegateX": {
                    "status_code": resp.status_code,
                    "response": resp_body,
                    "sentAt": datetime.datetime.utcnow().isoformat(),
                }
            })

        except Exception as e:
            print(f"❌ Error sending vote for {delegatex_user_id}: {e}")

    print("\n🎉 Batch voting completed.\n")


# ---------------------------------------------
# CLI
# ---------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("network", type=str)
    parser.add_argument("proposal_id", type=str)
    args = parser.parse_args()

    run_batch_vote(args.network.lower(), args.proposal_id)
