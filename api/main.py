# api/main.py

from fastapi import FastAPI, Request
from utils.firestore_helper import FirestoreHelper

app = FastAPI()
db = FirestoreHelper().db

@app.post("/create-bot")
async def create_bot(request: Request):
    body = await request.json()

    strategy_id = body.get("strategy_id")
    signature_link = body.get("signature_link")
    contact_link = body.get("contact_link")
    user_id = body.get("user_id")

    if not user_id or not strategy_id:
        return {"error": "Missing required fields"}

    # Store in Firestore using existing helper
    FirestoreHelper().create_user(
        delegatex_user_id=int(user_id),
        strategy_id=strategy_id,
        signature_link=signature_link,
        contact_link=contact_link
    )

    return {"status": "ok", "stored_user": user_id}
