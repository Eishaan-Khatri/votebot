# src/batch_vote.py

import os
import json
import requests
import datetime
import argparse
from utils.firestore_helper import FirestoreHelper


# ------------------------------------------------------------
# Firestore init
# ------------------------------------------------------------
def init_firestore():
    try:
        return FirestoreHelper().db
    except Exception as e:
        raise RuntimeError(f"Firestore initialization failed: {e}")


db = init_firestore()


# ------------------------------------------------------------
# Load first-line rationale from outputs.*.content
# ------------------------------------------------------------
def load_agent_reason(db, network, proposal_id, agent):
    """Reads: proposals/{network-proposalId}/outputs/{agent}.content"""
    doc_id = f"{network}-{proposal_id}"

    snap = db.collection("proposals").document(doc_id).get()
    if not snap.exists:
        return ""

    data = snap.to_dict()
    node = data.get("outputs", {}).get(agent, {})
    content = node.get("content")

    if not content:
        return ""

    try:
        parsed = json.loads(content)
    except:
        return ""

    rationale = parsed.get("rationale", "")
    if isinstance(rationale, str) and rationale.strip():
        return rationale.split(".")[0].strip()

    return ""


# ------------------------------------------------------------
# Convert decision → DelegateX numeric
# ------------------------------------------------------------
def decision_to_code(final_decision):
    d = final_decision.lower()
    if d == "aye":
        return 1
    if d == "nay":
        return 0
    return 2  # abstain fallback


# ------------------------------------------------------------
# MAIN EXECUTION
# ------------------------------------------------------------
def run_batch_vote(network: str, proposal_id: str):
    proposal_key = f"{network}-{proposal_id}"

    print(f"\n🔥 Running batch-vote for proposal {proposal_key}\n")

    users = db.collection("users").stream()

    for user in users:
        user_data = user.to_dict()
        delegatex_user_id = user_data.get("delegatexUserId")

        if not delegatex_user_id:
            print(f"⚠ Skipping user {user.id}: no delegatexUserId")
            continue

        # Load this user's vote for this proposal
        vote_doc = (
            db.collection("users")
            .document(str(delegatex_user_id))
            .collection("votes")
            .document(proposal_key)
            .get()
        )

        if not vote_doc.exists:
            print(f"⚠ No vote found for user {delegatex_user_id} on {proposal_key}")
            continue

        vote = vote_doc.to_dict()
        vote_blob = vote.get("vote", {}).get("content", "{}")

        try:
            vote_json = json.loads(vote_blob)
        except:
            print(f"❌ Invalid stored vote.content for {delegatex_user_id}")
            continue

        # Extract decision
        final_decision = vote_json.get("final_decision", "abstain")
        dx_decision = decision_to_code(final_decision)

        # Extract 3 agent first-line rationales
        r_balthazar = load_agent_reason(db, network, proposal_id, "balthazar")
        r_melchior  = load_agent_reason(db, network, proposal_id, "melchior")
        r_caspar    = load_agent_reason(db, network, proposal_id, "caspar")

        reasons = [r for r in [r_balthazar, r_melchior, r_caspar] if r]

        # DelegateX requires at least 1 reason
        if not reasons:
            reasons = ["Automated rationale unavailable."]

        # Comment (static)
        comment = f"Automated CyberGov vote: {final_decision.upper()}"

        # Payload according to DelegateX spec
        payload = {
            "userId": str(delegatex_user_id),
            "proposalId": str(proposal_id),
            "decision": dx_decision,
            "reason": reasons,       # array<string> (not object)
            "comment": comment,
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
        print("   URL:", url)
        print("   Headers:", json.dumps(headers, indent=2))
        print("   Payload:", json.dumps(payload, indent=2))

        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=20)
            print("   ⇦ Raw response:", resp.text)
            print(f"   DelegateX status: {resp.status_code}")
            resp_body = resp.text

            # Save API response
            vote_doc.reference.update(
                {
                    "delegateX": {
                        "status_code": resp.status_code,
                        "response": resp_body,
                        "sentAt": datetime.datetime.utcnow().isoformat(),
                    }
                }
            )

        except Exception as e:
            print(f"❌ Error sending vote for {delegatex_user_id}: {e}")

    print("\n🎉 Batch voting completed.\n")


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("network", type=str, help="polkadot | kusama | paseo")
    parser.add_argument("proposal_id", type=str, help="Proposal ID (e.g. 1797)")
    args = parser.parse_args()

    run_batch_vote(args.network.lower(), args.proposal_id)
