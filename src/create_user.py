from utils.firestore_helper import FirestoreHelper
import json

helper = FirestoreHelper()

delegatex_user_id = 23941  # Sample real user
proposal_id = "polkadot-1797"

# 1. Create the user
helper.create_user(
    delegatex_user_id=delegatex_user_id,
    strategy_id="neutral",
    signature_link="https://example.com/signature",
    contact_link="https://example.com/contact",
    wallet_address="5F....DOT_ADDRESS",
)

# 2. Create a fake vote document
user_ref = helper.db.collection("users").document(str(delegatex_user_id))
vote_ref = user_ref.collection("votes").document(proposal_id)

vote_content = {
    "final_decision": "Aye",
    "decision_reasoning": "Technical upgrade is essential.",
    "weighted_decision_metadata": {"strategy_used": "neutral"},
}

vote_ref.set({
    "vote": {
        "content": json.dumps(vote_content),
    },
    "comment": "CyberGov Bot voted AYE because this upgrade is essential.",
})

print("Sample user and vote created.")