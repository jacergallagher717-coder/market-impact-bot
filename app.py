"""
Market Impact & Trade Ideas Backend - COMPLETE PLATFORM
Features: Elite Filtering, Auth, Portfolio Tracking, Performance Dashboard
"""

from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from hashlib import sha256
import httpx
import os
import json
import asyncio
import xml.etree.ElementTree as ET
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2.pool import SimpleConnectionPool
import jwt
import bcrypt

# Import our custom modules
from market_enricher import get_enriched_context
from news_filter import should_send_alert, log_filter_decision, calculate_news_quality_score
from elite_prompt import get_elite_prompt_with_context

app = FastAPI(title="Market Impact API - COMPLETE PLATFORM 🚀")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== CONFIGURATION ====================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-4o"
DATABASE_URL = os.getenv("DATABASE_URL", "")
JWT_SECRET = os.getenv("JWT_SECRET", "your-secret-key-CHANGE-THIS-in-production")
JWT_ALGORITHM = "HS256"
JWT_EXPIRATION_HOURS = 24 * 7  # 1 week

MAX_ALERTS = 15
DAILY_ALERT_LIMIT = 10

# Global state
db_pool = None
seen_events = {}
recent_events = []
cnbc_monitor_running = False

# ==================== PLAYBOOK ====================

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

# ==================== PYDANTIC MODELS ====================

class NewsEvent(BaseModel):
    text: str
    source: str

class SignupRequest(BaseModel):
    email: EmailStr
    password: str
    display_name: Optional[str] = None

class LoginRequest(BaseModel):
    email: EmailStr
    password: str

class AuthResponse(BaseModel):
    token: str
    user: dict

class AddToPortfolioRequest(BaseModel):
    alert_id: int
    trade_idea_index: int
    entry_price: Optional[float] = None
    quantity: Optional[int] = 1
    notes: Optional[str] = None

class UpdatePortfolioRequest(BaseModel):
    exit_price: Optional[float] = None
    status: Optional[str] = None
    notes: Optional[str] = None

# ==================== AUTH HELPERS ====================

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_jwt_token(user_id: str, email: str) -> str:
    payload = {
        "user_id": user_id,
        "email": email,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRATION_HOURS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def verify_jwt_token(token: str) -> dict:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_current_user(authorization: Optional[str] = Header(None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            raise HTTPException(status_code=401, detail="Invalid authentication scheme")
        
        payload = verify_jwt_token(token)
        return payload
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid authorization header")

# ==================== DATABASE FUNCTIONS ====================

def init_database():
    global db_pool
    
    if not DATABASE_URL:
        print("⚠️  No DATABASE_URL found, running without persistence")
        return
    
    try:
        db_pool = SimpleConnectionPool(1, 10, DATABASE_URL)
        
        conn = db_pool.getconn()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id SERIAL PRIMARY KEY,
                alert_data JSONB NOT NULL,
                headline TEXT,
                category TEXT,
                source TEXT,
                detected_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_alerts_detected_at 
            ON alerts(detected_at DESC)
        """)
        
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        
        print("✅ Database initialized successfully")
        
    except Exception as e:
        print(f"❌ Database initialization error: {e}")
        db_pool = None

def save_alert_to_db(analysis: Dict[str, Any]):
    if not db_pool:
        return
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        event = analysis["event"]
        cursor.execute("""
            INSERT INTO alerts (alert_data, headline, category, source, detected_at)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            Json(analysis),
            event.get("headline"),
            event.get("category"),
            event.get("source"),
            datetime.fromisoformat(event.get("detected_at"))
        ))
        conn.commit()
        print("💾 Saved alert to Supabase database")
    except Exception as e:
        print(f"Error saving to database: {e}")
        conn.rollback()
    finally:
        cursor.close()
        db_pool.putconn(conn)

def load_alerts_from_db(limit: int = MAX_ALERTS) -> List[Dict]:
    if not db_pool:
        return []
    
    conn = db_pool.getconn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cursor.execute("""
            SELECT alert_data FROM alerts
            ORDER BY detected_at DESC
            LIMIT %s
        """, (limit,))
        
        rows = cursor.fetchall()
        print(f"📂 Loaded {len(rows)} alerts from Supabase database")
        return [dict(row["alert_data"]) for row in rows]
    except Exception as e:
        print(f"Error loading from database: {e}")
        return []
    finally:
        cursor.close()
        db_pool.putconn(conn)

def cleanup_old_alerts():
    if not db_pool:
        return
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cutoff_date = datetime.now() - timedelta(days=7)
        cursor.execute("""
            DELETE FROM alerts
            WHERE detected_at < %s
        """, (cutoff_date,))
        conn.commit()
    except Exception as e:
        print(f"Error cleaning up alerts: {e}")
    finally:
        cursor.close()
        db_pool.putconn(conn)

# ==================== PLAYBOOK FUNCTIONS ====================

def find_relevant_playbooks(news_text: str) -> List[Dict]:
    matches = []
    news_lower = news_text.lower()
    
    for name, playbook in PLAYBOOK.items():
        score = sum(1 for kw in playbook["keywords"] if kw.lower() in news_lower)
        if score > 0:
            matches.append({"name": name, "score": score, "playbook": playbook})
    
    return sorted(matches, key=lambda x: x["score"], reverse=True)

# ==================== AI AGENT ====================

async def call_openai_agent(news_text: str, playbook_context: str) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        return create_fallback_analysis(news_text)
    
    print("🔍 Fetching real-time market data...")
    enriched_context = await get_enriched_context(news_text)
    
    enhanced_playbook = f"{playbook_context}\n\n{enriched_context}"
    system_prompt = get_elite_prompt_with_context(enhanced_playbook)
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"Analyze this breaking news with ELITE institutional-level insight: {news_text}"}
                    ],
                    "temperature": 0.7
                }
            )
            response.raise_for_status()
            
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            
            if "```json" in content:
                content = content.split("```json")[1].split("```")[0]
            elif "```" in content:
                content = content.split("```")[1].split("```")[0]
            
            print("✅ Elite analysis complete with GPT-4o")
            return json.loads(content.strip())
            
        except Exception as e:
            print(f"OpenAI API error: {e}")
            return create_fallback_analysis(news_text)

def create_fallback_analysis(news_text: str) -> Dict[str, Any]:
    playbooks = find_relevant_playbooks(news_text)
    ticker = playbooks[0]["playbook"]["tickers"][0] if playbooks else "SPY"
    
    return {
        "event": {
            "headline": news_text[:100],
            "category": playbooks[0]["playbook"]["category"] if playbooks else "macro",
            "confidence": 0.5,
            "detected_at": datetime.now().isoformat(),
            "primary_direction": "neutral"
        },
        "why_it_matters": [
            "Market impact analysis unavailable",
            "Review news for trading opportunities",
            "Check related sectors for correlation"
        ],
        "trade_ideas": [{
            "ticker": ticker,
            "direction": "neutral",
            "strategy": "monitor",
            "rationale": "Awaiting detailed analysis",
            "conviction": "low",
            "entry_price": "TBD",
            "target_price": "TBD",
            "stop_loss": "TBD",
            "time_horizon": "TBD",
            "risk_reward_ratio": "1:1"
        }],
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

# ==================== TELEGRAM ====================

async def send_telegram_alert(analysis: Dict[str, Any], source: str) -> bool:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    
    event = analysis["event"]
    trade_ideas = analysis.get("trade_ideas", [])
    
    trade_list = "\n".join([
        f"{i+1}. {t['ticker']} - {t['strategy'].upper()} ({t['conviction']})"
        for i, t in enumerate(trade_ideas[:3])
    ])
    
    message = f"""🚨 *ELITE Market Alert* ({source})

{event['headline']}

*Direction:* {event.get('primary_direction', 'neutral').upper()}

*Top Trade Ideas:*
{trade_list}

*Why It Matters:*
{chr(10).join(f"• {b}" for b in analysis['why_it_matters'][:3])}

Confidence: {int(event.get('confidence', 0) * 100)}%

_Elite analysis - Not financial advice._"""
    
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

# ==================== NEWS PROCESSING ====================

def hash_event(text: str) -> str:
    return sha256(text.encode()).hexdigest()[:16]

def is_duplicate(event_hash: str) -> bool:
    if event_hash in seen_events:
        if datetime.now() - seen_events[event_hash] < timedelta(hours=24):
            return True
    seen_events[event_hash] = datetime.now()
    return False

async def process_news_item(headline: str, source: str):
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
        
        analysis = await call_openai_agent(headline, playbook_context)
        analysis["event"]["source"] = source
        analysis["event"]["detected_at"] = datetime.now().isoformat()
        
        # SMART FILTER
        should_send, rejection_reason = should_send_alert(headline, analysis)
        
        if not should_send:
            log_filter_decision(headline, analysis, sent=False, reason=rejection_reason)
            return
        
        quality_score = calculate_news_quality_score(headline, analysis)
        
        save_alert_to_db(analysis)
        await send_telegram_alert(analysis, source)
        
        recent_events = load_alerts_from_db(MAX_ALERTS)
        
        log_filter_decision(headline, analysis, sent=True)
        print(f"✅ Processed {source} news (Quality: {quality_score:.1%}): {headline[:60]}...")
        
    except Exception as e:
        print(f"Error processing news: {e}")

# ==================== CNBC MONITOR ====================

async def monitor_cnbc_rss():
    global cnbc_monitor_running
    cnbc_monitor_running = True
    
    seen_headlines = set()
    
    RSS_FEEDS = [
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069",
        "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10001147",
    ]
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    
    print(f"🚀 Starting CNBC monitor with ELITE filtering...")
    
    while cnbc_monitor_running:
        try:
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                for feed_url in RSS_FEEDS:
                    try:
                        response = await client.get(feed_url, headers=headers)
                        response.raise_for_status()
                        
                        root = ET.fromstring(response.content)
                        
                        for item in root.findall(".//item")[:5]:
                            title_elem = item.find("title")
                            if title_elem is not None and title_elem.text:
                                headline = title_elem.text.strip()
                                
                                if headline not in seen_headlines:
                                    seen_headlines.add(headline)
                                    await process_news_item(headline, "CNBC")
                    
                    except Exception as e:
                        print(f"Error fetching RSS feed: {e}")
                        continue
            
            await asyncio.sleep(30)
        
        except Exception as e:
            print(f"CNBC monitor error: {e}")
            await asyncio.sleep(60)

# ==================== AUTH ENDPOINTS ====================

@app.post("/api/auth/signup", response_model=AuthResponse)
async def signup(request: SignupRequest):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT id FROM user_profiles WHERE email = %s", (request.email,))
        if cursor.fetchone():
            raise HTTPException(status_code=400, detail="Email already registered")
        
        password_hash = hash_password(request.password)
        
        cursor.execute("""
            INSERT INTO user_profiles (email, display_name, last_login)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            RETURNING id, email, display_name, subscription_tier, created_at
        """, (request.email, request.display_name))
        
        user_data = cursor.fetchone()
        conn.commit()
        
        token = create_jwt_token(str(user_data[0]), user_data[1])
        
        return AuthResponse(
            token=token,
            user={
                "id": str(user_data[0]),
                "email": user_data[1],
                "display_name": user_data[2],
                "subscription_tier": user_data[3],
                "created_at": user_data[4].isoformat()
            }
        )
        
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Signup failed: {str(e)}")
    finally:
        cursor.close()
        db_pool.putconn(conn)

@app.post("/api/auth/login", response_model=AuthResponse)
async def login(request: LoginRequest):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT id, email, display_name, subscription_tier, created_at
            FROM user_profiles
            WHERE email = %s
        """, (request.email,))
        
        user_data = cursor.fetchone()
        if not user_data:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        cursor.execute("""
            UPDATE user_profiles SET last_login = CURRENT_TIMESTAMP WHERE id = %s
        """, (user_data[0],))
        conn.commit()
        
        token = create_jwt_token(str(user_data[0]), user_data[1])
        
        return AuthResponse(
            token=token,
            user={
                "id": str(user_data[0]),
                "email": user_data[1],
                "display_name": user_data[2],
                "subscription_tier": user_data[3],
                "created_at": user_data[4].isoformat()
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login failed: {str(e)}")
    finally:
        cursor.close()
        db_pool.putconn(conn)

@app.get("/api/auth/me")
async def get_me(current_user: dict = Depends(get_current_user)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT id, email, display_name, subscription_tier, created_at, last_login
            FROM user_profiles
            WHERE id = %s
        """, (current_user["user_id"],))
        
        user_data = cursor.fetchone()
        if not user_data:
            raise HTTPException(status_code=404, detail="User not found")
        
        return {
            "id": str(user_data[0]),
            "email": user_data[1],
            "display_name": user_data[2],
            "subscription_tier": user_data[3],
            "created_at": user_data[4].isoformat(),
            "last_login": user_data[5].isoformat() if user_data[5] else None
        }
        
    finally:
        cursor.close()
        db_pool.putconn(conn)

# ==================== PORTFOLIO ENDPOINTS ====================

@app.get("/api/portfolio")
async def get_portfolio(current_user: dict = Depends(get_current_user)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                p.id, p.alert_id, p.trade_idea_index, p.ticker, p.direction, p.strategy,
                p.entry_price, p.quantity, p.entry_date, p.exit_price, p.exit_date,
                p.status, p.pnl_percent, p.pnl_dollars, p.notes, p.created_at,
                a.alert_data
            FROM user_portfolios p
            LEFT JOIN alerts a ON a.id = p.alert_id
            WHERE p.user_id = %s
            ORDER BY p.created_at DESC
        """, (current_user["user_id"],))
        
        rows = cursor.fetchall()
        
        portfolio = []
        for row in rows:
            portfolio.append({
                "id": str(row[0]),
                "alert_id": row[1],
                "trade_idea_index": row[2],
                "ticker": row[3],
                "direction": row[4],
                "strategy": row[5],
                "entry_price": float(row[6]) if row[6] else None,
                "quantity": row[7],
                "entry_date": row[8].isoformat(),
                "exit_price": float(row[9]) if row[9] else None,
                "exit_date": row[10].isoformat() if row[10] else None,
                "status": row[11],
                "pnl_percent": float(row[12]) if row[12] else None,
                "pnl_dollars": float(row[13]) if row[13] else None,
                "notes": row[14],
                "created_at": row[15].isoformat(),
                "alert_data": row[16]
            })
        
        return {"portfolio": portfolio, "total": len(portfolio)}
        
    finally:
        cursor.close()
        db_pool.putconn(conn)

@app.post("/api/portfolio/add")
async def add_to_portfolio(request: AddToPortfolioRequest, current_user: dict = Depends(get_current_user)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT alert_data FROM alerts WHERE id = %s", (request.alert_id,))
        alert_row = cursor.fetchone()
        if not alert_row:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        alert_data = alert_row[0]
        trade_ideas = alert_data.get("trade_ideas", [])
        
        if request.trade_idea_index >= len(trade_ideas):
            raise HTTPException(status_code=400, detail="Invalid trade idea index")
        
        trade_idea = trade_ideas[request.trade_idea_index]
        
        entry_price = request.entry_price if request.entry_price else trade_idea.get("entry_price", "0")
        if isinstance(entry_price, str):
            if "-" in entry_price:
                low, high = entry_price.split("-")
                entry_price = (float(low) + float(high)) / 2
            else:
                try:
                    entry_price = float(entry_price)
                except:
                    entry_price = 0
        
        cursor.execute("""
            INSERT INTO user_portfolios (
                user_id, alert_id, trade_idea_index, ticker, direction, strategy,
                entry_price, quantity, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, alert_id, trade_idea_index) DO NOTHING
            RETURNING id
        """, (
            current_user["user_id"],
            request.alert_id,
            request.trade_idea_index,
            trade_idea.get("ticker"),
            trade_idea.get("direction"),
            trade_idea.get("strategy"),
            entry_price,
            request.quantity,
            request.notes
        ))
        
        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=400, detail="Trade already in portfolio")
        
        conn.commit()
        
        return {"success": True, "portfolio_id": str(result[0]), "message": "Added to portfolio"}
        
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to add to portfolio: {str(e)}")
    finally:
        cursor.close()
        db_pool.putconn(conn)

@app.patch("/api/portfolio/{portfolio_id}")
async def update_portfolio(portfolio_id: str, request: UpdatePortfolioRequest, current_user: dict = Depends(get_current_user)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        updates = []
        params = []
        
        if request.exit_price is not None:
            updates.append("exit_price = %s")
            params.append(request.exit_price)
            updates.append("exit_date = CURRENT_TIMESTAMP")
        
        if request.status:
            updates.append("status = %s")
            params.append(request.status)
        
        if request.notes is not None:
            updates.append("notes = %s")
            params.append(request.notes)
        
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        if request.exit_price:
            updates.append("pnl_percent = ((exit_price - entry_price) / NULLIF(entry_price, 0)) * 100")
            updates.append("pnl_dollars = (exit_price - entry_price) * quantity")
        
        updates.append("updated_at = CURRENT_TIMESTAMP")
        
        params.extend([portfolio_id, current_user["user_id"]])
        
        query = f"""
            UPDATE user_portfolios
            SET {', '.join(updates)}
            WHERE id = %s AND user_id = %s
            RETURNING id
        """
        
        cursor.execute(query, params)
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Portfolio trade not found")
        
        conn.commit()
        
        return {"success": True, "message": "Portfolio updated"}
        
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to update portfolio: {str(e)}")
    finally:
        cursor.close()
        db_pool.putconn(conn)

@app.delete("/api/portfolio/{portfolio_id}")
async def delete_from_portfolio(portfolio_id: str, current_user: dict = Depends(get_current_user)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            DELETE FROM user_portfolios
            WHERE id = %s AND user_id = %s
            RETURNING id
        """, (portfolio_id, current_user["user_id"]))
        
        result = cursor.fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Portfolio trade not found")
        
        conn.commit()
        
        return {"success": True, "message": "Removed from portfolio"}
        
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to remove from portfolio: {str(e)}")
    finally:
        cursor.close()
        db_pool.putconn(conn)

# ==================== PERFORMANCE ENDPOINTS ====================

@app.get("/api/performance/global")
async def get_global_performance():
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT * FROM calculate_global_performance()")
        result = cursor.fetchone()
        
        if not result:
            return {
                "total_alerts": 0,
                "avg_confidence": 0,
                "total_portfolio_trades": 0,
                "win_rate": 0,
                "avg_return": 0
            }
        
        return {
            "total_alerts": result[0],
            "avg_confidence": float(result[1]) if result[1] else 0,
            "total_portfolio_trades": result[2],
            "win_rate": float(result[3]) if result[3] else 0,
            "avg_return": float(result[4]) if result[4] else 0
        }
        
    finally:
        cursor.close()
        db_pool.putconn(conn)

@app.get("/api/performance/user")
async def get_user_performance(current_user: dict = Depends(get_current_user)):
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not available")
    
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT 
                COUNT(*) as total_trades,
                COUNT(*) FILTER (WHERE status = 'open') as open_trades,
                COUNT(*) FILTER (WHERE status = 'closed') as closed_trades,
                COUNT(*) FILTER (WHERE pnl_percent > 0) as winning_trades,
                COUNT(*) FILTER (WHERE pnl_percent < 0) as losing_trades,
                AVG(pnl_percent) FILTER (WHERE status = 'closed') as avg_return,
                SUM(pnl_dollars) FILTER (WHERE status = 'closed') as total_pnl,
                MAX(pnl_percent) as best_trade_percent,
                MIN(pnl_percent) as worst_trade_percent
            FROM user_portfolios
            WHERE user_id = %s
        """, (current_user["user_id"],))
        
        result = cursor.fetchone()
        
        total_closed = result[2] or 0
        winning = result[3] or 0
        
        return {
            "total_trades": result[0],
            "open_trades": result[1],
            "closed_trades": total_closed,
            "winning_trades": winning,
            "losing_trades": result[4] or 0,
            "win_rate": (winning / total_closed * 100) if total_closed > 0 else 0,
            "avg_return": float(result[5]) if result[5] else 0,
            "total_pnl": float(result[6]) if result[6] else 0,
            "best_trade_percent": float(result[7]) if result[7] else 0,
            "worst_trade_percent": float(result[8]) if result[8] else 0
        }
        
    finally:
        cursor.close()
        db_pool.putconn(conn)

# ==================== ALERTS ENDPOINTS ====================

@app.get("/api/alerts")
async def get_alerts():
    return {"alerts": recent_events}

@app.post("/api/news")
async def submit_news(event: NewsEvent, background_tasks: BackgroundTasks):
    background_tasks.add_task(process_news_item, event.text, event.source)
    return {"status": "processing", "message": "News submitted for analysis"}

# ==================== HEALTH & ROOT ====================

@app.get("/")
async def root():
    return {"status": "ok", "message": f"Market Impact API - {len(recent_events)} elite alerts"}

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "telegram_configured": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID),
        "openai_configured": bool(OPENAI_API_KEY),
        "alpha_vantage_configured": bool(os.getenv("ALPHA_VANTAGE_API_KEY")),
        "database_configured": bool(db_pool),
        "cnbc_monitor_active": cnbc_monitor_running,
        "alerts_count": len(recent_events),
        "storage": "Supabase PostgreSQL" if db_pool else "In-Memory",
        "mode": "🔥 COMPLETE PLATFORM - Auth + Portfolio + Elite Filtering",
        "features": [
            "Smart News Filtering",
            "User Authentication",
            "Portfolio Tracking",
            "Performance Analytics",
            "Real-time Market Data",
            "Elite AI Analysis"
        ]
    }

# ==================== STARTUP/SHUTDOWN ====================

@app.on_event("startup")
async def startup_event():
    init_database()
    
    global recent_events
    recent_events = load_alerts_from_db(MAX_ALERTS)
    
    cleanup_old_alerts()
    
    asyncio.create_task(monitor_cnbc_rss())
    print(f"✅ COMPLETE PLATFORM started - {len(recent_events)} elite alerts loaded")

@app.on_event("shutdown")
async def shutdown_event():
    global cnbc_monitor_running, db_pool
    cnbc_monitor_running = False
    
    if db_pool:
        db_pool.closeall()
    
    print("⏹️  System shut down")
