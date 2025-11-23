
TEMPLATES = {
    "aggressive": {"balthazar": 0.3, "caspar": 0.2, "melchior": 0.5},
    "risk-averse": {"balthazar": 0.2, "caspar": 0.6, "melchior": 0.2},
    "conservative": {"balthazar": 0.3, "caspar": 0.5, "melchior": 0.2},
    "growth-oriented": {"balthazar": 0.3, "caspar": 0.2, "melchior": 0.5},
    "technical": {"balthazar": 0.4, "caspar": 0.4, "melchior": 0.2},
    "community-focused": {"balthazar": 0.2, "caspar": 0.3, "melchior": 0.5},
    "treasury-watchdog": {"balthazar": 0.2, "caspar": 0.6, "melchior": 0.2},
    "validator-aligned": {"balthazar": 0.4, "caspar": 0.4, "melchior": 0.2},
    "neutral": {"balthazar": 0.33, "caspar": 0.33, "melchior": 0.34},
    "experimental": {"balthazar": 0.25, "caspar": 0.15, "melchior": 0.60},
}

def get_strategy_weights(strategy_id: str):
    """Returns the weights for a given strategy ID, or Neutral if not found."""
    return TEMPLATES.get(strategy_id.lower(), TEMPLATES["neutral"])
