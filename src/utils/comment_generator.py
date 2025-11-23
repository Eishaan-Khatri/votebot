
"""
Comment Generator for CyberGov
Generates concise 1-2 line comments for Polkassembly from vote data using Gemini.
"""

from typing import Dict, Any, List
import os
import logging
import google.generativeai as genai

logger = logging.getLogger(__name__)

def _call_gemini_for_summary(prompt: str) -> str:
    """
    Call Gemini API to generate summary.
    
    Args:
        prompt: Prompt for Gemini
    
    Returns:
        Generated summary text
    """
    # Prioritize GEMINI_API_KEY as per user instruction, fallback to OPENROUTER if that's what they have set
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("OPENROUTER_API_KEY")
    
    if not api_key:
        raise ValueError("GEMINI_API_KEY (or OPENROUTER_API_KEY) not found in environment")
    
    try:
        genai.configure(api_key=api_key)
        # Using the model specified by user, or fallback to standard
        model_name = "gemini-2.0-flash-exp" 
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        logger.error(f"Gemini API call failed: {e}")
        raise

def generate_comment(vote_data: Dict[str, Any], agent_reasons: Dict[str, list]) -> str:
    """
    Generate a concise 1-2 line comment from vote data using Gemini to summarize agent rationales.
    
    Args:
        vote_data: Parsed vote.json content (or dict representation of WeightedDecisionResult)
        agent_reasons: Agent rationales from extract_agent_reasons()
    
    Returns:
        Short comment string (1-2 lines)
    """
    
    final_decision = vote_data.get("final_decision", "Abstain")
    metadata = vote_data.get("weighted_decision_metadata", {})
    strategy = metadata.get("strategy_used", "neutral")
    
    # Get agent rationales
    balthazar_rationale = agent_reasons.get("balthazar", [""])[0][:500]  # Limit to 500 chars
    melchior_rationale = agent_reasons.get("melchior", [""])[0][:500]
    caspar_rationale = agent_reasons.get("caspar", [""])[0][:500]
    
    # Create prompt for Gemini
    prompt = f"""You are summarizing a governance vote analysis. Create a concise 1-2 sentence summary for a public comment.

Decision: {final_decision}
Strategy: {strategy}

Agent Analysis:
- Balthazar (Strategic): {balthazar_rationale}
- Melchior (Growth): {melchior_rationale}
- Caspar (Risk): {caspar_rationale}

Write a professional 1-2 sentence summary explaining the vote decision and key reasoning. Start with "CyberGov Bot voted {final_decision.upper()}". Keep it under 150 characters if possible."""

    try:
        # Call Gemini API
        summary = _call_gemini_for_summary(prompt)
        
        # Clean up the response
        summary = summary.strip()
        
        # Ensure it starts correctly
        if not summary.startswith("CyberGov"):
            summary = f"CyberGov Bot voted {final_decision.upper()}. {summary}"
        
        return summary
        
    except Exception as e:
        # Fallback to simple comment if Gemini fails
        logger.warning(f"Gemini API failed, using fallback comment: {e}")
        decision_reasoning = metadata.get("decision_reasoning", "")
        return f"CyberGov Bot voted {final_decision.upper()} using {strategy} strategy. {decision_reasoning}"


def generate_comment(vote_data: Dict[str, Any], proposal_doc: Dict[str, Any]) -> str:
    agent_reasons = extract_agent_reasons(proposal_doc)    """
    Extract agent rationales from Firestore proposal document.
    
    Args:
        proposal_doc: Full Firestore proposal document
    
    Returns:
        Dict with agent names as keys and list of rationale strings as values
        Format: {"caspar": ["rationale"], "melchior": ["rationale"], "balthazar": ["rationale"]}
    """
    
    reasons = {
        "caspar": [],
        "melchior": [],
        "balthazar": []
    }
    
    # Access files.outputs which contains the agent JSONs
    files = proposal_doc.get("files", {})
    outputs = files.get("outputs", {})
    
    for agent_name in ["caspar", "melchior", "balthazar"]:
        agent_data = outputs.get(agent_name, {})
        
        # Parse JSON content if it's a string (which it is in our schema)
        content = agent_data.get("content", {})
        if isinstance(content, str):
            import json
            try:
                content = json.loads(content)
            except:
                content = {}
        
        rationale = content.get("rationale", "")
        if rationale:
            reasons[agent_name] = [rationale]
    
    return reasons


def format_delegatex_payload(
    user_id: str,
    proposal_id: str,
    vote_data: Dict[str, Any],
    agent_reasons: Dict[str, list],
    comment: str
) -> Dict[str, Any]:
    """
    Format the complete payload for DelegateX API.
    """
    
    # Map decision to number: 0=NAY, 1=AYE, 2=ABSTAIN
    final_decision = vote_data.get("final_decision", "Abstain")
    decision_map = {
        "Nay": 0,
        "Aye": 1,
        "Abstain": 2
    }
    decision_number = decision_map.get(final_decision, 2)
    
    payload = {
        "user_id": user_id,
        "proposal_id": proposal_id,
        "decision": decision_number,
        "reason": agent_reasons,
        "comment": comment
    }
    
    return payload
