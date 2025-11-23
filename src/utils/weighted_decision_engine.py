"""
WeightedGovernanceDecisionEngine_v1
Template-weighted tri-agent voting system with safety overrides.
"""

from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass
import logging
from .strategies import TEMPLATES, get_strategy_weights

logger = logging.getLogger(__name__)

# Thresholds
LOW_MARGIN_THRESHOLD = 0.15
ABSTAIN_DOMINANCE_THRESHOLD = 0.40
WEAK_CONFIDENCE_THRESHOLD = 0.60
MODERATE_CONFIDENCE_THRESHOLD = 0.70


@dataclass
class WeightedDecisionResult:
    """Result of weighted decision computation."""
    final_decision: str  # "Aye", "Nay", or "Abstain"
    weighted_scores: Dict[str, float]
    margin: float
    confidence: str  # "Strong", "Moderate", "Weak", "Abstain"
    rules_triggered: List[str]
    decision_reasoning: str
    template_used: str
    template_weights: Dict[str, float]
    agent_votes: List[Dict[str, Any]]
    engine_version: str = "WeightedGovernanceDecisionEngine_v1"


def compute_weighted_decision(
    agent_votes: List[Dict[str, str]],
    template_name: str = "neutral",
    custom_weights: Optional[Dict[str, float]] = None
) -> WeightedDecisionResult:
    """
    Compute final decision using weighted governance engine.
    
    Args:
        agent_votes: List of dicts with 'agent' and 'decision' keys
                    e.g., [{"agent": "balthazar", "decision": "Aye"}, ...]
        template_name: Name of template to use (default: "neutral")
        custom_weights: Optional custom weights (overrides template)
    
    Returns:
        WeightedDecisionResult with full decision metadata
    """
    
    # Step 1: Get template weights
    if custom_weights:
        template_weights = custom_weights
        template_used = "custom"
    else:
        template_used = template_name.lower()
        template_weights = get_strategy_weights(template_used)
    
    # Normalize weights to sum to 1.0 (safety check)
    total_weight = sum(template_weights.values()) if template_weights else 0.0
    if total_weight == 0.0:
        logger.warning("Template weights sum to zero or are empty — falling back to neutral template.")
        template_weights = get_strategy_weights("neutral")
        total_weight = sum(template_weights.values())
    
    if abs(total_weight - 1.0) > 0.01:
        template_weights = {k: float(v) / total_weight for k, v in template_weights.items()}
    
    # Warn about missing expected agents
    for expected in ("balthazar", "caspar", "melchior"):
        if expected not in template_weights:
            logger.warning("Template '%s' missing weight for agent '%s' — treating as 0.0", template_used, expected)
    
    # Step 2: Compute weighted scores
    weighted_scores = {"Aye": 0.0, "Nay": 0.0, "Abstain": 0.0}
    
    for vote in agent_votes:
        agent = str(vote.get("agent", "")).lower()
        decision = vote.get("decision", "")
        weight = template_weights.get(agent, 0.0)
        
        # Normalize decision string robustly
        dnorm = str(decision).strip().lower()
        if dnorm in ("aye", "yes", "y"):
            decision_key = "Aye"
        elif dnorm in ("nay", "no", "n"):
            decision_key = "Nay"
        else:
            decision_key = "Abstain"
        
        weighted_scores[decision_key] += weight
    
    # Step 3: Determine ranking
    sorted_scores = sorted(weighted_scores.items(), key=lambda x: x[1], reverse=True)
    top_decision = sorted_scores[0][0]
    top_score = sorted_scores[0][1]
    runner_up_score = sorted_scores[1][1] if len(sorted_scores) > 1 else 0.0
    margin = top_score - runner_up_score
    
    # Step 4: Apply decision rules
    rules_triggered = []
    final_decision = top_decision
    decision_reasoning = ""
    
    # Rule 1: Low-Margin Override
    if margin < LOW_MARGIN_THRESHOLD:
        rules_triggered.append("low_margin_override")
        final_decision = "Abstain"
        decision_reasoning = f"Low margin ({margin:.3f} < {LOW_MARGIN_THRESHOLD}); defaulting to Abstain for safety"
    
    # Rule 2: Abstain Dominance
    elif weighted_scores["Abstain"] >= ABSTAIN_DOMINANCE_THRESHOLD or top_decision == "Abstain":
        if weighted_scores["Abstain"] >= ABSTAIN_DOMINANCE_THRESHOLD:
            rules_triggered.append("abstain_dominance_threshold")
            decision_reasoning = f"Abstain score ({weighted_scores['Abstain']:.3f}) exceeds dominance threshold ({ABSTAIN_DOMINANCE_THRESHOLD})"
        else:
            rules_triggered.append("abstain_highest")
            decision_reasoning = f"Abstain has highest weighted score ({weighted_scores['Abstain']:.3f})"
        final_decision = "Abstain"
    
    # Rule 3: Weak Confidence Override
    elif top_score < WEAK_CONFIDENCE_THRESHOLD:
        rules_triggered.append("weak_confidence_override")
        final_decision = "Abstain"
        decision_reasoning = f"Top score ({top_score:.3f}) below confidence threshold ({WEAK_CONFIDENCE_THRESHOLD}); defaulting to Abstain"
    
    # Rule 4: Tie Rule (multiple decisions with same top score)
    elif len([s for s in sorted_scores if s[1] == top_score]) > 1:
        rules_triggered.append("tie_rule")
        final_decision = "Abstain"
        decision_reasoning = f"Multiple decisions tied at {top_score:.3f}; defaulting to Abstain"
    
    # Rule 5: Clear winner
    else:
        rules_triggered.append("weighted_decision")
        decision_reasoning = f"{top_decision} has clear weighted majority ({top_score:.3f}) with margin {margin:.3f}"
    
    # Step 5: Classify confidence
    if final_decision == "Abstain" and any(r in rules_triggered for r in ["low_margin_override", "abstain_dominance_threshold", "abstain_highest", "weak_confidence_override", "tie_rule"]):
        confidence = "Abstain"
    elif top_score >= MODERATE_CONFIDENCE_THRESHOLD:
        confidence = "Strong"
    elif top_score >= WEAK_CONFIDENCE_THRESHOLD:
        confidence = "Moderate"
    else:
        confidence = "Weak"
    
    # Build agent votes with weights applied
    agent_votes_with_weights = []
    for vote in agent_votes:
        agent = str(vote.get("agent", "")).lower()
        agent_votes_with_weights.append({
            "agent": vote.get("agent", "unknown"),
            "vote": vote.get("decision", "Abstain"),
            "weight_applied": template_weights.get(agent, 0.0)
        })
    
    return WeightedDecisionResult(
        final_decision=final_decision,
        weighted_scores=weighted_scores,
        margin=margin,
        confidence=confidence,
        rules_triggered=rules_triggered,
        decision_reasoning=decision_reasoning,
        template_used=template_used,
        template_weights=template_weights,
        agent_votes=agent_votes_with_weights
    )


def get_template_weights(template_name: str) -> Dict[str, float]:
    """Get weights for a specific template."""
    return get_strategy_weights(template_name)


def list_available_templates() -> List[str]:
    """List all available template names."""
    return list(TEMPLATES.keys())
