"""
Market Impact & Trade Ideas Backend with CNBC Auto-Monitoring (ALL NEWS VERSION)
"""

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from hashlib import sha256
import httpx
import os
import json
import asyncio
import xml.etree.ElementTree as ET

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
cnbc_monitor_running = False

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
    },
    "tech_earnings": {
        "keywords": ["earnings", "revenue", "profit", "guidance", "quarterly", "beat", "miss", "Tesla", "Apple", "Microsoft", "Amazon", "Google", "Meta", "Netflix"],
        "tickers": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NFLX"],
        "category": "sector",
        "description": "Tech earnings and guidance"
    },
    "fed_rates": {
        "keywords": ["Federal Reserve", "Fed", "interest rate", "Powell", "FOMC", "inflation", "CPI", "jobs report"],
        "tickers": ["TLT", "IEF", "GLD", "DXY", "SPY"],
        "category": "macro",
        "description": "Fed policy and rates"
    },
    "housing": {
        "keywords": ["housing", "home sales", "mortgage", "real estate", "construction"],
        "tickers": ["XHB", "ITB", "DHI", "LEN", "PHM"],
        "category": "sector",
        "description": "Housing and real estate"
    },
    "banks": {
        "keywords": ["bank", "JPMorgan", "Wells Fargo", "Bank of America", "lending", "deposits"],
        "tickers": ["JPM", "BAC", "WFC", "C", "GS", "MS", "XLF"],
        "category": "sector",
        "description": "Banking sector"
    },
    "stock_market": {
        "keywords": ["stock", "futures", "S&P", "Dow", "Nasdaq", "market", "rally", "selloff", "earnings week"],
        "tickers": ["SPY", "QQQ", "DIA", "IWM", "VIX"],
        "category": "macro",
        "description": "General stock market movements"
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
    "confidence": 0.0-1.0,
    "detected_at": "ISO timestamp"
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
        "event": {
            "headline": text[:100], 
            "category": "general", 
            "confidence": 0.3,
            "detected_at": datetime.now().isoformat()
        },
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


async def process_news_item(headline: str, source: str):
    """Process a single news item through the analysis pipeline - ALL NEWS VERSION"""
    try:
        event_hash = hash_event(headline)
        if is_duplicate(event_hash):
            return
        
        # Process ALL breaking news, not just playbook matches
        playbooks = find_relevant_playbooks(headline)
        
        playbook_context = "\n".join(
            f"- {p['playbook']['description']}: {', '.join(p['playbook']['tickers'][:4])}"
            for p in playbooks
        ) if playbooks else "General market news - identify affected sectors and tickers from the headline"
        
        analysis = await call_anthropic_agent(headline, playbook_context)
        analysis["event"]["source"] = source
        analysis["event"]["detected_at"] = datetime.now().isoformat()
        
        recent_events.insert(0, analysis)
        if len(recent_events) > 100:
            recent_events.pop()
        
        # Send to Telegram if configured
        await send_telegram_alert(analysis, source)
        
        print(f"✅ Processed {source} news: {headline[:60]}...")
        
    except Exception as e:
        print(f"Error processing news: {e}")


async def monitor_cnbc_rss():
    """Monitor CNBC RSS feed for breaking news with proper headers"""
    global cnbc_monitor_running
    cnbc_monitor_running = True
    
    seen_headlines = set()
    
    # Multiple RSS feed options
    RSS_FEEDS = [
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",  # Top News
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069",  # Breaking News
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147",  # US News
    ]
    
    # Browser-like headers to avoid 403
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    
    print("🚀 Starting CNBC monitor - ALL breaking news will be processed...")
    
    while cnbc_monitor_running:
        try:
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                # Try each feed
                for feed_url in RSS_FEEDS:
                    try:
                        response = await client.get(feed_url, headers=headers)
                        response.raise_for_status()
                        
                        # Parse RSS XML
                        root = ET.fromstring(response.content)
                        
                        # Extract items from RSS feed (handle different RSS formats)
                        items = root.findall('.//item') or root.findall('.//{http://search.cnbc.com/rs/search/combinedcms/view}item')
                        
                        for item in items:
                            title_elem = item.find('title') or item.find('{http://search.cnbc.com/rs/search/combinedcms/view}title')
                            if title_elem is not None and title_elem.text:
                                headline = title_elem.text.strip()
                                
                                # Skip if we've seen this headline
                                if headline in seen_headlines:
                                    continue
                                
                                seen_headlines.add(headline)
                                
                                # Keep seen_headlines size manageable
                                if len(seen_headlines) > 200:
                                    seen_headlines.pop()
                                
                                # Process ALL news - no filtering
                                await process_news_item(headline, "CNBC")
                        
                        # Successfully processed a feed, no need to try others
                        break
                        
                    except httpx.HTTPStatusError as e:
                        print(f"Feed {feed_url} returned {e.response.status_code}, trying next...")
                        continue
                    except Exception as e:
                        print(f"Error with feed {feed_url}: {e}")
                        continue
                
        except Exception as e:
            print(f"CNBC monitor error: {e}")
        
        # Check every 2 minutes
        await asyncio.sleep(120)


@app.on_event("startup")
async def startup_event():
    """Start background tasks on startup"""
    asyncio.create_task(monitor_cnbc_rss())
    print("✅ CNBC monitor task started - ALL BREAKING NEWS MODE")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global cnbc_monitor_running
    cnbc_monitor_running = False
    print("⏹️  CNBC monitor stopped")


@app.get("/")
async def root():
    return {"status": "ok", "message": "Market Impact API with CNBC Auto-Monitoring (ALL NEWS)"}


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "telegram_configured": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID),
        "anthropic_configured": bool(ANTHROPIC_API_KEY),
        "cnbc_monitor_active": cnbc_monitor_running,
        "events_processed": len(recent_events),
        "alerts_available": len(recent_events),
        "mode": "ALL_BREAKING_NEWS"
    }


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    """Webhook for Telegram bot messages"""
    try:
        body = await request.json()
        message = body.get("message", {}) or body.get("channel_post", {})
        text = message.get("text", "")
        
        if not text or text.startswith("/"):
            return {"status": "ignored"}
        
        await process_news_item(text, "telegram")
        
        return {"status": "processed"}
        
    except Exception as e:
        print(f"Telegram webhook error: {e}")
        return {"status": "error", "message": str(e)}


@app.get("/events")
async def get_events(limit: int = 20):
    """Get recent events (legacy endpoint)"""
    return {"events": recent_events[:limit]}


@app.get("/api/alerts")
async def get_alerts():
    """Public endpoint for website to fetch alerts"""
    return {
        "alerts": recent_events[:50],
        "count": len(recent_events)
    }


@app.post("/api/test-alert")
async def test_alert(news: NewsEvent):
    """Manual endpoint to test with custom news"""
    await process_news_item(news.text, news.source)
    return {"status": "processed", "message": "Alert created"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
