"""
Professional Market Data Enricher - Polygon.io Integration
Provides real-time stock prices, company news, and market context
"""

import httpx
import os
from typing import Dict, List, Any
from datetime import datetime, timedelta
import asyncio

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY", "")

async def get_stock_price(ticker: str) -> Dict[str, Any]:
    """Get real-time stock price from Polygon"""
    if not POLYGON_API_KEY:
        return {}
    
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url, params={"apiKey": POLYGON_API_KEY})
            response.raise_for_status()
            data = response.json()
            
            if data.get("results"):
                result = data["results"][0]
                return {
                    "ticker": ticker,
                    "price": result.get("c", 0),  # close price
                    "open": result.get("o", 0),
                    "high": result.get("h", 0),
                    "low": result.get("l", 0),
                    "volume": result.get("v", 0),
                    "change": round(((result.get("c", 0) - result.get("o", 0)) / result.get("o", 1)) * 100, 2)
                }
        except Exception as e:
            print(f"Error fetching price for {ticker}: {e}")
    
    return {}


async def get_company_news(ticker: str, limit: int = 3) -> List[Dict[str, Any]]:
    """Get latest company news from Polygon"""
    if not POLYGON_API_KEY:
        return []
    
    # Get date range (last 7 days)
    today = datetime.now().strftime("%Y-%m-%d")
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    url = f"https://api.polygon.io/v2/reference/news"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url, params={
                "ticker": ticker,
                "published_utc.gte": week_ago,
                "published_utc.lte": today,
                "limit": limit,
                "order": "desc",
                "apiKey": POLYGON_API_KEY
            })
            response.raise_for_status()
            data = response.json()
            
            news_items = []
            for article in data.get("results", [])[:limit]:
                news_items.append({
                    "title": article.get("title", ""),
                    "publisher": article.get("publisher", {}).get("name", ""),
                    "url": article.get("article_url", ""),
                    "published": article.get("published_utc", "")
                })
            
            return news_items
        except Exception as e:
            print(f"Error fetching news for {ticker}: {e}")
    
    return []


async def get_market_snapshot() -> Dict[str, Any]:
    """Get major market indices snapshot"""
    if not POLYGON_API_KEY:
        return {}
    
    indices = {
        "SPY": "S&P 500",
        "QQQ": "Nasdaq",
        "DIA": "Dow Jones"
    }
    
    snapshot = {}
    
    for ticker, name in indices.items():
        price_data = await get_stock_price(ticker)
        if price_data:
            snapshot[name] = {
                "ticker": ticker,
                "price": price_data.get("price", 0),
                "change": price_data.get("change", 0)
            }
    
    return snapshot


def extract_tickers_from_news(news_text: str) -> List[str]:
    """Extract potential ticker symbols from news text"""
    # Common tickers that appear in news
    common_tickers = {
        "Tesla": "TSLA", "Apple": "AAPL", "Microsoft": "MSFT", "Amazon": "AMZN",
        "Google": "GOOGL", "Meta": "META", "Netflix": "NFLX", "NVIDIA": "NVDA",
        "JPMorgan": "JPM", "Bank of America": "BAC", "Goldman": "GS", 
        "Ford": "F", "GM": "GM", "Boeing": "BA", "Caterpillar": "CAT",
        "Walmart": "WMT", "Target": "TGT", "Costco": "COST",
        "Intel": "INTC", "AMD": "AMD", "Qualcomm": "QCOM",
        "Pfizer": "PFE", "Moderna": "MRNA", "Johnson": "JNJ",
        "Exxon": "XOM", "Chevron": "CVX", "ConocoPhillips": "COP",
        "Bitcoin": "BTC-USD", "Ethereum": "ETH-USD"
    }
    
    tickers = []
    news_lower = news_text.lower()
    
    for company, ticker in common_tickers.items():
        if company.lower() in news_lower:
            tickers.append(ticker)
    
    return tickers[:5]  # Limit to 5 most relevant


async def get_enriched_context(news_text: str) -> str:
    """
    Get enriched market context for the news
    Returns formatted string with real-time data
    """
    if not POLYGON_API_KEY:
        return "\n⚠️ Market data enrichment unavailable (no Polygon API key)"
    
    print(f"🔍 Enriching with real-time Polygon.io data: {news_text[:60]}...")
    
    # Extract relevant tickers
    tickers = extract_tickers_from_news(news_text)
    
    # Get market snapshot
    market_snapshot_task = get_market_snapshot()
    
    # Get stock prices for relevant tickers
    stock_tasks = [get_stock_price(ticker) for ticker in tickers[:3]]
    
    # Get news for most relevant ticker
    news_task = get_company_news(tickers[0]) if tickers else None
    
    # Execute all requests concurrently
    results = await asyncio.gather(
        market_snapshot_task,
        *stock_tasks,
        news_task if news_task else asyncio.sleep(0),
        return_exceptions=True
    )
    
    market_snapshot = results[0] if isinstance(results[0], dict) else {}
    stock_prices = [r for r in results[1:len(stock_tasks)+1] if isinstance(r, dict) and r]
    company_news = results[-1] if news_task and isinstance(results[-1], list) else []
    
    # Build enriched context
    context_parts = []
    
    # Market snapshot
    if market_snapshot:
        context_parts.append("\n📊 **CURRENT MARKET CONDITIONS:**")
        for index_name, data in market_snapshot.items():
            change_emoji = "🟢" if data["change"] > 0 else "🔴"
            context_parts.append(
                f"  {change_emoji} {index_name} ({data['ticker']}): ${data['price']:.2f} ({data['change']:+.2f}%)"
            )
    
    # Stock prices for relevant tickers
    if stock_prices:
        context_parts.append("\n💰 **RELEVANT STOCK PRICES (Real-Time):**")
        for stock in stock_prices:
            change_emoji = "🟢" if stock["change"] > 0 else "🔴"
            context_parts.append(
                f"  {change_emoji} {stock['ticker']}: ${stock['price']:.2f} "
                f"(Open: ${stock['open']:.2f}, High: ${stock['high']:.2f}, Low: ${stock['low']:.2f}) "
                f"Change: {stock['change']:+.2f}%"
            )
    
    # Recent company news
    if company_news:
        context_parts.append(f"\n📰 **RECENT NEWS FOR {tickers[0]}:**")
        for i, article in enumerate(company_news[:2], 1):
            context_parts.append(f"  {i}. {article['title']} ({article['publisher']})")
    
    enriched = "\n".join(context_parts)
    
    stock_count = len(stock_prices)
    news_count = len(company_news)
    print(f"✅ Enriched with {stock_count} real-time stock prices, {news_count} news items")
    
    return enriched if enriched else "\n⚠️ No relevant market data found for this news"


# Fallback for when no API key
async def get_enriched_context_fallback(news_text: str) -> str:
    """Fallback when Polygon API is not available"""
    return """
⚠️ Real-time market data unavailable. Using analysis based on news content only.

To enable real-time stock prices and market data:
1. Sign up for free API at https://polygon.io/
2. Add POLYGON_API_KEY to environment variables
3. Restart the service
"""


# Main export
if not POLYGON_API_KEY:
    print("⚠️ WARNING: POLYGON_API_KEY not set. Real-time market data will be unavailable.")
    print("   Sign up for free at https://polygon.io/ to enable professional market data.")
