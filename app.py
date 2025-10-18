"""
Market Impact & Trade Ideas Backend
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from hashlib import sha256
import httpx
import os
import json

app = FastAPI(title="Market Impact API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-sonnet-4-5-20250929"

seen_events = {}
recent_events = []

PLAYBOOK = {
    "soybeans": {
        "keywords": ["soybean", "cooking oil", "ag policy", "crop", "agricultural"],
        "tickers": ["ZS", "BO", "BG", "ADM", "CAG", "DBA"],
        "category": "commodity",
        "description": "Soybean/cooking oil supply chain"
    },
    "ai_datacenter": {
        "keywords": ["AI", "datacenter", "capex", "chips", "GPU", "power", "NVIDIA", "semiconductor"],
        "tickers": ["NVDA", "SMCI", "DELL", "HPE", "VRT", "TT", "CARR", "GNRC"],
        "category": "sector",
        "description": "AI infrastructure and datacenter buildout"
    },
    "tariffs": {
        "keywords": ["tariff", "China", "trade war", "import", "export ban", "sanctions"],
        "tickers": ["FXI", "MCHI", "CAT", "DE", "STLD", "NUE"],
        "category": "macro",
        "description": "Trade policy and tariffs"
    },
    "crude_energy": {
        "keywords": ["crude", "oil", "OPEC", "supply cut", "production", "energy"],
        "tickers": ["CL", "XLE", "XOP", "XOM", "CVX"],
        "category": "commodity",
        "description": "Crude oil and energy markets"
    }
}

AGENT_SYSTEM_PROMPT = """You are a real-time market impact analyst and trade ideation engine.

Given breaking news, you must:
1. Classify the event type
2. Explain why it matters (3-5 bullet points)
3. List affected assets with direction and time horizon
4. Propose 1-2 risk-aware trade ideas

PLAYBOOK KNOWLEDGE:
{playbook_context}

OUTPUT: Valid JSON matching this schema:
{{
  "event": {{
    "headline": "concise headline",
    "category": "macro|commodity|sector",
    "confidence": 0.0-1.0
  }},
  "why_it_matters": ["point 1", "point 2", "point 3"],
  "affected_assets": [
    {{"ticker": "SYMBOL", "direction": "up|down", "rationale": "brief reason"}}
  ],
  "trade_idea": {{
    "ticker": "SYMBOL",
    "direction": "bullish|bearish",
    "strategy": "shares|calls|puts|spread",
    "rationale": "why this trade makes sense",
    "risk": "main risk factor"
  }}
}}

Output ONLY valid JSON."""


class NewsEvent(BaseModel):
    text: str
    source: str


def hash_event(text: str) -> str:
    return sha256(text.encode()).hexdigest()[:16]


def is_duplicate(event_hash: str, window_hours: int = 24) -> bool:
    if event_hash in seen_events:
        seen_time = seen_events[event_hash]
        if datetime.now() - seen_time < timedelta(hours=window_hours):
            return True
    seen_events[event_hash] = datetime.now()
    return False


def find_relevant_playbooks(text: str, top_k: int = 3) -> List[Dict[str, Any]]:
    text_lower = text.lower()
    matches = []
    
    for name, pb in PLAYBOOK.items():
        score = sum(1 for kw in pb["keywords"] if kw in text_lower)
        if score > 0:
            matches.append({"name": name, "score": score, "playbook": pb})
    
    matches.sort(key=lambda x: x["score"], reverse=True)
    return matches[:top_k]


async def call_anthropic_agent(news_text: str, playbook_context: str) -> Dict[str, Any]:
    if not ANTHROPIC_API_KEY:
        return create_fallback_analysis(news_text)
    
    system_prompt = AGENT_SYSTEM_PROMPT.format(playbook_context=playbook_context)
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": ANTHROPIC_MODEL,
                    "max_tokens": 2048,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": f"Analyze: {news_text}"}]
                }
            )
            response.raise_for_status()
            
            result = response.json()
            content = result["content"][0]["text"]
            
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            
            return json.loads(content.strip())
            
        except Exception as e:
            print(f"Anthropic API error: {e}")
            return create_fallback_analysis(news_text)


def create_fallback_analysis(text: str) -> Dict[str, Any]:
    playbooks = find_relevant_playbooks(text)
    ticker = playbooks[0]["playbook"]["tickers"][0] if playbooks else "SPY"
    
    return {
        "event": {"headline": text[:100], "category": "general", "confidence": 0.3},
        "why_it_matters": ["News detected but analysis unavailable"],
        "affected_assets": [{"ticker": ticker, "direction": "neutral", "rationale": "Manual review needed"}],
        "trade_idea": {"ticker": ticker, "direction": "neutral", "strategy": "monitor", "rationale": "Await confirmation", "risk": "Unclear impact"}
    }


async def send_telegram_alert(analysis: Dict[str, Any], source: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    event = analysis["event"]
    trade = analysis.get("trade_idea", {})
    
    message = f"""🚨 *Market Alert* ({source})

{event['headline']}

*Why It Matters:*
{chr(10).join(f"• {b}" for b in analysis['why_it_matters'][:3])}

*Trade Idea:*
{trade.get('ticker', 'N/A')} - {trade.get('direction', 'N/A').upper()}
Strategy: {trade.get('strategy', 'monitor')}
{trade.get('rationale', '')}

⚠️ Risk: {trade.get('risk', 'See full analysis')}

Confidence: {int(event.get('confidence', 0) * 100)}%

_Not financial advice._"""
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                url,
                json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
            )
            response.raise_for_status()
            return True
        except Exception as e:
            print(f"Telegram error: {e}")
            return False


@app.get("/")
async def root():
    return {"status": "ok", "message": "Market Impact API"}


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "telegram_configured": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID),
        "anthropic_configured": bool(ANTHROPIC_API_KEY),
        "events_processed": len(recent_events)
    }


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    try:
        body = await request.json()
        message = body.get("message", {}) or body.get("channel_post", {})
        text = message.get("text", "")
        
        if not text or text.startswith("/"):
            return {"status": "ignored"}
        
        event_hash = hash_event(text)
        if is_duplicate(event_hash):
            return {"status": "duplicate"}
        
        playbooks = find_relevant_playbooks(text)
        playbook_context = "\n".join(
            f"- {p['playbook']['description']}: {', '.join(p['playbook']['tickers'][:4])}"
            for p in playbooks
        )
        
        analysis = await call_anthropic_agent(text, playbook_context)
        analysis["event"]["source"] = "telegram"
        
        recent_events.insert(0, analysis)
        if len(recent_events) > 50:
            recent_events.pop()
        
        await send_telegram_alert(analysis, "telegram")
        
        return {"status": "processed", "confidence": analysis["event"]["confidence"]}
        
    except Exception as e:
        print(f"Error: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/events")
async def get_events(limit: int = 20):
    return {"events": recent_events[:limit]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)



