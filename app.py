"""
Market Impact & Trade Ideas Backend - MULTI-TRADE + PERSISTENT STORAGE + REAL-TIME DATA
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
from pathlib import Path
from market_enricher import get_enriched_context

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

# Persistent storage file
STORAGE_FILE = Path("/tmp/market_alerts.json")
MAX_ALERTS = 15

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
1. Determine the PRIMARY market direction (bullish or bearish)
2. Generate 3-5 SPECIFIC trade ideas in that SAME direction with different tickers
3. Explain why it matters (3-5 bullet points)
4. Provide bull/bear/base case scenarios with affected assets

PLAYBOOK KNOWLEDGE:
{playbook_context}

OUTPUT: Valid JSON matching this schema:
{{
  "event": {{
    "headline": "concise headline",
    "category": "macro|commodity|sector",
    "confidence": 0.0-1.0,
    "detected_at": "ISO timestamp",
    "primary_direction": "bullish|bearish"
  }},
  "why_it_matters": ["point 1", "point 2", "point 3"],
  "trade_ideas": [
    {{
      "ticker": "SYMBOL",
      "direction": "bullish|bearish",
      "strategy": "shares|calls|puts|spread",
      "rationale": "why this specific trade",
      "conviction": "high|medium|low"
    }}
  ],
  "scenarios": {{
    "bull_case": {{
      "description": "What happens in best case scenario",
      "probability": "percentage like 30%",
      "affected_assets": [
        {{"ticker": "SYMBOL", "impact": "up|down", "magnitude": "strong|moderate|weak"}}
      ]
    }},
    "bear_case": {{
      "description": "What happens in worst case scenario",
      "probability": "percentage like 20%",
      "affected_assets": [
        {{"ticker": "SYMBOL", "impact": "up|down", "magnitude": "strong|moderate|weak"}}
      ]
    }},
    "base_case": {{
      "description": "Most likely outcome",
      "probability": "percentage like 50%",
      "affected_assets": [
        {{"ticker": "SYMBOL", "impact": "up|down", "magnitude": "strong|moderate|weak"}}
      ]
    }}
  }}
}}

IMPORTANT: 
- Generate 3-5 trade ideas ALL IN THE SAME DIRECTION (all bullish OR all bearish)
- Each trade idea should be a DIFFERENT ticker with unique rationale
- If news is bullish for sector, give multiple bullish stock ideas in that sector
- If news is bearish, give multiple bearish plays (puts, shorts, inverse ETFs)
- Order trade ideas by conviction (highest first)

Output ONLY valid JSON."""


class NewsEvent(BaseModel):
    text: str
    source: str


def load_alerts_from_storage():
    """Load alerts from persistent storage"""
    global recent_events
    try:
        if STORAGE_FILE.exists():
            with open(STORAGE_FILE, 'r') as f:
                data = json.load(f)
                recent_events = data.get('alerts', [])
                print(f"📂 Loaded {len(recent_events)} alerts from storage")
        else:
            recent_events = []
            print("📂 No existing alerts found, starting fresh")
    except Exception as e:
        print(f"❌ Error loading alerts: {e}")
        recent_events = []


def save_alerts_to_storage():
    """Save alerts to persistent storage"""
    try:
        with open(STORAGE_FILE, 'w') as f:
            json.dump({'alerts': recent_events[:MAX_ALERTS]}, f)
        print(f"💾 Saved {len(recent_events)} alerts to storage")
    except Exception as e:
        print(f"❌ Error saving alerts: {e}")


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
    
    # NEW: Get enriched market data with real-time prices, news, and context
    print("🔍 Fetching real-time market data...")
    enriched_context = await get_enriched_context(news_text)
    
    # Build enhanced system prompt with real-time data
    enhanced_playbook = f"{playbook_context}\n\n{enriched_context}"
    system_prompt = AGENT_SYSTEM_PROMPT.format(playbook_context=enhanced_playbook)
    
    async with httpx.AsyncClient(timeout=45.0) as client:
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
                    "max_tokens": 4096,
                    "system": system_prompt,
                    "messages": [{"role": "user", "content": f"Analyze this breaking news and provide multiple trade ideas in the same direction: {news_text}"}]
                }
            )
            response.raise_for_status()
            
            result = response.json()
            content = result["content"][0]["text"]
            
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            
            print("✅ Analysis complete with real-time data")
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
            "detected_at": datetime.now().isoformat(),
            "primary_direction": "neutral"
        },
        "why_it_matters": ["News detected but full analysis unavailable"],
        "trade_ideas": [
            {
                "ticker": ticker,
                "direction": "neutral",
                "strategy": "monitor",
                "rationale": "Await confirmation and full analysis",
                "conviction": "low"
            }
        ],
        "scenarios": {
            "bull_case": {
                "description": "Positive interpretation of news",
                "probability": "33%",
                "affected_assets": [{"ticker": ticker, "impact": "up", "magnitude": "moderate"}]
            },
            "bear_case": {
                "description": "Negative interpretation of news",
                "probability": "33%",
                "affected_assets": [{"ticker": ticker, "impact": "down", "magnitude": "moderate"}]
            },
            "base_case": {
                "description": "Neutral market reaction",
                "probability": "34%",
                "affected_assets": [{"ticker": ticker, "impact": "neutral", "magnitude": "weak"}]
            }
        }
    }


async def send_telegram_alert(analysis: Dict[str, Any], source: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    event = analysis["event"]
    trade_ideas = analysis.get("trade_ideas", [])
    
    trade_list = "\n".join([
        f"{i+1}. {t['ticker']} - {t['strategy'].upper()} ({t['conviction']})"
        for i, t in enumerate(trade_ideas[:3])
    ])
    
    message = f"""🚨 *Market Alert* ({source})

{event['headline']}

*Direction:* {event.get('primary_direction', 'neutral').upper()}

*Top Trade Ideas:*
{trade_list}

*Why It Matters:*
{chr(10).join(f"• {b}" for b in analysis['why_it_matters'][:3])}

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
    """Process news and maintain 15-alert limit with FIFO"""
    global recent_events
    try:
        event_hash = hash_event(headline)
        if is_duplicate(event_hash):
            return
        
        playbooks = find_relevant_playbooks(headline)
        
        playbook_context = "\n".join(
            f"- {p['playbook']['description']}: {', '.join(p['playbook']['tickers'][:6])}"
            for p in playbooks
        ) if playbooks else "General market news - identify multiple trade opportunities in the same direction"
        
        analysis = await call_anthropic_agent(headline, playbook_context)
        analysis["event"]["source"] = source
        analysis["event"]["detected_at"] = datetime.now().isoformat()
        
        # Add to front of list
        recent_events.insert(0, analysis)
        
        # Keep only MAX_ALERTS (15), oldest ones get pushed out
        if len(recent_events) > MAX_ALERTS:
            removed = recent_events[MAX_ALERTS:]
            recent_events = recent_events[:MAX_ALERTS]
            print(f"🗑️  Removed {len(removed)} oldest alert(s) to maintain {MAX_ALERTS} limit")
        
        # Save to persistent storage
        save_alerts_to_storage()
        
        await send_telegram_alert(analysis, source)
        
        print(f"✅ Processed {source} news ({len(recent_events)}/{MAX_ALERTS} alerts): {headline[:60]}...")
        
    except Exception as e:
        print(f"Error processing news: {e}")


async def monitor_cnbc_rss():
    """Monitor CNBC RSS feed for breaking news"""
    global cnbc_monitor_running
    cnbc_monitor_running = True
    
    seen_headlines = set()
    
    RSS_FEEDS = [
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147",
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }
    
    print(f"🚀 Starting CNBC monitor - Maintaining {MAX_ALERTS} alerts at all times...")
    
    while cnbc_monitor_running:
        try:
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                for feed_url in RSS_FEEDS:
                    try:
                        response = await client.get(feed_url, headers=headers)
                        response.raise_for_status()
                        
                        root = ET.fromstring(response.content)
                        items = root.findall('.//item') or root.findall('.//{http://search.cnbc.com/rs/search/combinedcms/view}item')
                        
                        for item in items:
                            title_elem = item.find('title') or item.find('{http://search.cnbc.com/rs/search/combinedcms/view}title')
                            if title_elem is not None and title_elem.text:
                                headline = title_elem.text.strip()
                                
                                if headline in seen_headlines:
                                    continue
                                
                                seen_headlines.add(headline)
                                
                                if len(seen_headlines) > 200:
                                    seen_headlines.pop()
                                
                                await process_news_item(headline, "CNBC")
                        
                        break
                        
                    except httpx.HTTPStatusError as e:
                        print(f"Feed returned {e.response.status_code}, trying next...")
                        continue
                    except Exception as e:
                        print(f"Error with feed: {e}")
                        continue
                
        except Exception as e:
            print(f"CNBC monitor error: {e}")
        
        await asyncio.sleep(120)


@app.on_event("startup")
async def startup_event():
    # Load existing alerts from storage first
    load_alerts_from_storage()
    
    # Then start the monitor
    asyncio.create_task(monitor_cnbc_rss())
    print(f"✅ CNBC monitor started - {len(recent_events)}/{MAX_ALERTS} alerts loaded")


@app.on_event("shutdown")
async def shutdown_event():
    global cnbc_monitor_running
    cnbc_monitor_running = False
    
    # Save alerts before shutdown
    save_alerts_to_storage()
    print("⏹️  CNBC monitor stopped, alerts saved")


@app.get("/")
async def root():
    return {"status": "ok", "message": f"Market Impact API - {len(recent_events)}/{MAX_ALERTS} alerts"}


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "telegram_configured": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID),
        "anthropic_configured": bool(ANTHROPIC_API_KEY),
        "cnbc_monitor_active": cnbc_monitor_running,
        "alerts_count": len(recent_events),
        "max_alerts": MAX_ALERTS,
        "mode": "PERSISTENT_15_ALERTS_WITH_REALTIME_DATA"
    }


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
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
    return {"events": recent_events[:limit]}


@app.get("/api/alerts")
async def get_alerts():
    return {
        "alerts": recent_events,
        "count": len(recent_events),
        "max": MAX_ALERTS
    }


@app.post("/api/test-alert")
async def test_alert(news: NewsEvent):
    await process_news_item(news.text, news.source)
    return {"status": "processed", "message": f"Alert created ({len(recent_events)}/{MAX_ALERTS} total)"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
