"""
Elite News Filter - Only High-Impact, Actionable Alerts
Filters out generic market commentary and noise
"""

import re
from typing import Dict, Optional

# Patterns that indicate LOW-QUALITY, generic news
NOISE_PATTERNS = [
    r"stocks making the biggest moves",
    r"stocks making the most moves",
    r"biggest movers",
    r"top gainers",
    r"top losers",
    r"stocks to watch",
    r"daily market recap",
    r"market wrap",
    r"pre-market",
    r"after-hours",
    r"midday",
    r"here's what",
    r"here are the",
    r"stocks on the move",
]

# Keywords that indicate HIGH-QUALITY, actionable news
SIGNAL_KEYWORDS = [
    # Corporate events
    "earnings", "beats", "misses", "guidance", "outlook", "forecast",
    "merger", "acquisition", "buyout", "takeover", "deal",
    "ceo", "resignation", "appointed", "executive", "leadership",
    "layoffs", "hiring", "expansion", "restructuring",
    
    # Regulatory/Legal
    "fda", "approval", "rejected", "investigation", "lawsuit", "settlement",
    "regulation", "antitrust", "sec", "ftc",
    
    # Financial
    "dividend", "buyback", "debt", "funding", "investment", "raise",
    "bankruptcy", "default", "downgrade", "upgrade", "rating",
    
    # Product/Technology
    "launch", "announced", "unveiled", "patent", "breakthrough",
    "recall", "defect", "shortage", "supply chain",
    
    # Market events
    "halt", "suspended", "delisted", "ipo", "listing",
    "crash", "surge", "plunge", "soars", "tumbles",
    
    # Macro events with specific impact
    "tariff", "sanctions", "trade war", "ban", "restriction",
    "interest rate", "fed", "inflation", "gdp",
]

# Minimum requirements for alert quality
MIN_CONVICTION = 0.5  # AI must have 50%+ confidence
MIN_TRADE_IDEAS = 2    # Must have at least 2 distinct trade ideas


def is_noise(headline: str) -> bool:
    """Check if headline is generic noise"""
    headline_lower = headline.lower()
    
    # Check for noise patterns
    for pattern in NOISE_PATTERNS:
        if re.search(pattern, headline_lower):
            return True
    
    return False


def has_signal(headline: str) -> bool:
    """Check if headline contains actionable signal"""
    headline_lower = headline.lower()
    
    # Must have at least one signal keyword
    for keyword in SIGNAL_KEYWORDS:
        if keyword in headline_lower:
            return True
    
    return False


def calculate_news_quality_score(headline: str, analysis: Dict) -> float:
    """
    Calculate quality score (0-1) for news alert
    Higher score = more valuable, actionable alert
    """
    score = 0.0
    headline_lower = headline.lower()
    
    # Base score from AI confidence
    confidence = analysis.get("event", {}).get("confidence", 0)
    score += confidence * 0.3  # 30% weight
    
    # Has specific company/ticker mentioned
    if any(ticker in headline.upper() for ticker in ["AAPL", "TSLA", "NVDA", "MSFT", "GOOGL", "META", "AMZN"]):
        score += 0.15
    
    # Contains catalyst keywords
    catalyst_count = sum(1 for keyword in SIGNAL_KEYWORDS if keyword in headline_lower)
    score += min(catalyst_count * 0.1, 0.2)  # Up to 20% for catalysts
    
    # Number of trade ideas
    trade_ideas = len(analysis.get("trade_ideas", []))
    if trade_ideas >= 3:
        score += 0.15
    elif trade_ideas >= 2:
        score += 0.10
    
    # Has specific time horizon
    if any(idea.get("time_horizon") not in ["N/A", ""] for idea in analysis.get("trade_ideas", [])):
        score += 0.1
    
    # Has specific entry/exit prices (not TBD)
    specific_prices = sum(
        1 for idea in analysis.get("trade_ideas", [])
        if idea.get("entry_price", "TBD") != "TBD" and idea.get("target_price", "TBD") != "TBD"
    )
    if specific_prices >= 2:
        score += 0.1
    
    return min(score, 1.0)  # Cap at 1.0


def should_send_alert(headline: str, analysis: Dict) -> tuple[bool, Optional[str]]:
    """
    Determine if alert meets quality threshold
    Returns: (should_send, reason_if_rejected)
    """
    # Check 1: Is it noise?
    if is_noise(headline):
        return False, "Generic market commentary (noise pattern detected)"
    
    # Check 2: Does it have signal?
    if not has_signal(headline):
        return False, "No actionable catalyst identified"
    
    # Check 3: AI conviction too low?
    confidence = analysis.get("event", {}).get("confidence", 0)
    if confidence < MIN_CONVICTION:
        return False, f"Low AI confidence: {confidence:.1%} (minimum: {MIN_CONVICTION:.1%})"
    
    # Check 4: Not enough trade ideas?
    trade_ideas = len(analysis.get("trade_ideas", []))
    if trade_ideas < MIN_TRADE_IDEAS:
        return False, f"Only {trade_ideas} trade ideas (minimum: {MIN_TRADE_IDEAS})"
    
    # Check 5: Overall quality score
    quality_score = calculate_news_quality_score(headline, analysis)
    if quality_score < 0.6:  # Minimum 60% quality score
        return False, f"Quality score too low: {quality_score:.1%} (minimum: 60%)"
    
    # Passed all checks!
    return True, None


def get_alert_summary(headline: str, analysis: Dict) -> str:
    """Get summary of why alert passed/failed"""
    quality_score = calculate_news_quality_score(headline, analysis)
    confidence = analysis.get("event", {}).get("confidence", 0)
    trade_ideas = len(analysis.get("trade_ideas", []))
    
    return (
        f"Quality: {quality_score:.1%} | "
        f"Confidence: {confidence:.1%} | "
        f"Trade Ideas: {trade_ideas}"
    )


# Example usage in logs:
def log_filter_decision(headline: str, analysis: Dict, sent: bool, reason: Optional[str] = None):
    """Log why an alert was sent or filtered"""
    summary = get_alert_summary(headline, analysis)
    
    if sent:
        print(f"✅ SENT: {headline[:80]}... | {summary}")
    else:
        print(f"❌ FILTERED: {headline[:80]}... | {summary} | Reason: {reason}")
