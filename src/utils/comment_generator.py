# src/utils/comment_generator.py

import json
import os
import logging
from typing import Dict, List, Any

import google.generativeai as genai

logger = logging.getLogger(__name__)


# -------------------------------------------------------------
# 1. Extract agent reasons from Firestore proposal doc
# -------------------------------------------------------------
def extract_agent_reasons(proposal_data):
    outputs = proposal_data.get("files", {}).get("outputs", {})

    cleaned = {}

    for agent in ["balthazar", "melchior", "caspar"]:
        try:
            node = outputs.get(agent, {})
            content = node.get("content")

            if not content:
                cleaned[agent] = []
                continue

            # JSON-parse if needed
            if isinstance(content, str):
                content = json.loads(content)

            rationale = content.get("rationale", "")

            # --- FORCE ALWAYS LIST OF STRINGS ---
            if isinstance(rationale, list):
                cleaned[agent] = [str(x) for x in rationale]

            elif isinstance(rationale, str):
                cleaned[agent] = [rationale]

            else:
                cleaned[agent] = []

        except Exception as e:
            logger.error(f"Bad rationale for {agent}: {e}")
            cleaned[agent] = []

    return cleaned


# -------------------------------------------------------------
# 2. Gemini API wrapper
# -------------------------------------------------------------
def _call_gemini(prompt: str) -> str:
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("Missing GEMINI_API_KEY or OPENROUTER_API_KEY")

    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel("gemini-1.5-flash")
        response = model.generate_content(prompt)
        return (response.text or "").strip()

    except Exception as e:
        logger.error(f"Gemini API error: {e}")
        raise


# -------------------------------------------------------------
# 3A. Short comment
# -------------------------------------------------------------
def _generate_comment_short(final_decision: str, agent_reasons: Dict[str, List[str]]) -> str:
    """
    Generates a clean 1–2 sentence human-readable rationale using Gemini.
    Does NOT mention strategy. Synthesizes all agents.
    """
    b = (agent_reasons.get("balthazar", [""])[0])[:500]
    m = (agent_reasons.get("melchior", [""])[0])[:500]
    c = (agent_reasons.get("caspar", [""])[0])[:500]

    prompt = f"""
You are writing a short governance explanation.

Decision: {final_decision}

Rationales:
- {b}
- {m}
- {c}

Write 1–2 sentences.
Start with: "CyberGov Bot voted {final_decision.upper()} because ..."
Do NOT mention agents by name.
Do NOT mention weights or templates.
Length target: 80–140 characters.
"""

    try:
        summary = _call_gemini(prompt).strip()
        if not summary.lower().startswith("cybergov"):
            summary = f"CyberGov Bot voted {final_decision.upper()} because {summary}"
        return summary.replace("\n", " ").strip()
    except Exception as e:
        logger.error(f"Gemini comment error: {e}")
        return f"CyberGov Bot voted {final_decision.upper()} because the analysis supported this outcome."


# -------------------------------------------------------------
# 3B. Long technical comment (optional)
# -------------------------------------------------------------
def _generate_comment_long(final_decision: str, strategy: str, agent_reasons: Dict[str, List[str]]) -> str:
    def fmt(agent):
        rs = agent_reasons.get(agent, [])
        return "\n".join(f"- {x}" for x in rs) if rs else "No rationale provided."

    return f"""
### CyberGov Bot — Final Vote Explanation

**Decision:** {final_decision}  
**Strategy Applied:** {strategy}

---

### Balthazar (Strategic)
{fmt("balthazar")}

### Melchior (Growth)
{fmt("melchior")}

### Caspar (Risk)
{fmt("caspar")}
""".strip()


# -------------------------------------------------------------
# 3C. Unified entrypoint
# -------------------------------------------------------------
def generate_comment(
    vote_data: Dict[str, Any],
    agent_reasons: Dict[str, List[str]],
    mode: str = "short",
) -> str:
    """
    Main entrypoint used by batch_vote.py.
    """
    final_decision = vote_data.get("final_decision", "Abstain")
    strategy = vote_data.get("weighted_decision_metadata", {}).get(
        "strategy_used", "neutral"
    )

    if mode == "long":
        return _generate_comment_long(final_decision, strategy, agent_reasons)

    return _generate_comment_short(final_decision, agent_reasons)


# -------------------------------------------------------------
# 4. DelegateX payload builder
# -------------------------------------------------------------
def format_delegatex_payload(
    user_id: str,
    proposal_id: str,
    vote_data: Dict[str, Any],
    agent_reasons: Dict[str, list],
    comment: str,
) -> Dict[str, Any]:

    decision_map = {"Nay": 0, "Aye": 1, "Abstain": 2}
    final_decision = vote_data.get("final_decision", "Abstain")

    return {
        "userId": user_id,         # DelegateX user ID (NOT Firestore)
        "proposalId": proposal_id,
        "decision": decision_map.get(final_decision, 2),
        "reason": {
            "balthazar": agent_reasons.get("balthazar", []),
            "melchior": agent_reasons.get("melchior", []),
            "caspar": agent_reasons.get("caspar", []),
        },
        "comment": comment,
    }
