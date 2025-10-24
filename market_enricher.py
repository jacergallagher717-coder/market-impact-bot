"""
Professional Market Data Enricher - Alpha Vantage Integration
Provides real-time stock prices, company news, and market context
"""

import httpx
import os
from typing import Dict, List, Any
from datetime import datetime
import asyncio

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")

async def get_stock_quote(ticker: str) -> Dict[str, Any]:
    """Get real-time stock quote from Alpha Vantage"""
    if not ALPHA_VANTAGE_API_KEY:
        return {}
    
    url = "https://www.alphavantage.co/query"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url, params={
                "function": "GLOBAL_QUOTE",
                "symbol": ticker,
                "apikey": ALPHA_VANTAGE_API_KEY
            })
            response.raise_for_status()
            data = response.json()
            
            quote = data.get("Global Quote", {})
            if quote:
                price = float(quote.get("05. price", 0))
                open_price = float(quote.get("02. open", 0))
                high = float(quote.get("03. high", 0))
                low = float(quote.get("04. low", 0))
                volume = int(quote.get("06. volume", 0))
                change_percent = float(quote.get("10. change percent", "0").replace("%", ""))
                
                return {
                    "ticker": ticker,
                    "price": round(price, 2),
                    "open": round(open_price, 2),
                    "high": round(high, 2),
                    "low": round(low, 2),
                    "volume": volume,
                    "change": round(change_percent, 2)
                }
        except Exception as e:
            print(f"Error fetching quote for {ticker}: {e}")
    
    return {}


async def get_company_overview(ticker: str) -> Dict[str, Any]:
    """Get company overview from Alpha Vantage"""
    if not ALPHA_VANTAGE_API_KEY:
        return {}
    
    url = "https://www.alphavantage.co/query"
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url, params={
                "function": "OVERVIEW",
                "symbol": ticker,
                "apikey": ALPHA_VANTAGE_API_KEY
            })
            response.raise_for_status()
            data = response.json()
            
            if data and "Symbol" in data:
                return {
                    "name": data.get("Name", ""),
                    "sector": data.get("Sector", ""),
                    "industry": data.get("Industry", ""),
                    "market_cap": data.get("MarketCapitalization", ""),
                    "pe_ratio": data.get("PERatio", "N/A")
                }
        except Exception as e:
            print(f"Error fetching overview for {ticker}: {e}")
    
    return {}


async def get_market_news(tickers: List[str] = None) -> List[Dict[str, Any]]:
    """Get market news from Alpha Vantage"""
    if not ALPHA_VANTAGE_API_KEY:
        return []
    
    url = "https://www.alphavantage.co/query"
    
    # Get general market news
    params = {
        "function": "NEWS_SENTIMENT",
        "apikey": ALPHA_VANTAGE_API_KEY,
        "limit": 5
    }
    
    # Add tickers if provided
    if tickers:
        params["tickers"] = ",".join(tickers[:3])  # Limit to 3 tickers
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            news_items = []
            for article in data.get("feed", [])[:3]:
                news_items.append({
                    "title": article.get("title", ""),
                    "summary": article.get("summary", "")[:150] + "...",
                    "source": article.get("source", ""),
                    "sentiment": article.get("overall_sentiment_label", "Neutral"),
                    "url": article.get("url", "")
                })
            
            return news_items
        except Exception as e:
            print(f"Error fetching news: {e}")
    
    return []


def extract_tickers_from_news(news_text: str) -> List[str]:
    """Extract potential ticker symbols from news text"""
    # Common tickers that appear in news
    ticker_map = {
        "tesla": "TSLA", "apple": "AAPL", "microsoft": "MSFT", "amazon": "AMZN",
        "google": "GOOGL", "alphabet": "GOOGL", "meta": "META", "facebook": "META",
        "netflix": "NFLX", "nvidia": "NVDA", "amd": "AMD", "intel": "INTC",
        "jpmorgan": "JPM", "bank of america": "BAC", "goldman": "GS", "morgan stanley": "MS",
        "ford": "F", "gm": "GM", "general motors": "GM", "boeing": "BA", "caterpillar": "CAT",
        "walmart": "WMT", "target": "TGT", "costco": "COST", "home depot": "HD",
        "qualcomm": "QCOM", "broadcom": "AVGO", "cisco": "CSCO",
        "pfizer": "PFE", "moderna": "MRNA", "johnson": "JNJ", "merck": "MRK",
        "exxon": "XOM", "chevron": "CVX", "conocophillips": "COP",
        "visa": "V", "mastercard": "MA", "paypal": "PYPL",
        "disney": "DIS", "comcast": "CMCSA", "warner": "WBD",
        "coca-cola": "KO", "pepsi": "PEP", "starbucks": "SBUX",
        "nike": "NKE", "lululemon": "LULU", "under armour": "UAA"
    }
    
    tickers = []
    news_lower = news_text.lower()
    
    for keyword, ticker in ticker_map.items():
        if keyword in news_lower and ticker not in tickers:
            tickers.append(ticker)
    
    return tickers[:5]  # Limit to 5 most relevant


async def get_enriched_context(news_text: str) -> str:
    """
    Get enriched market context for the news
    Returns formatted string with real-time data
    """
    if not ALPHA_VANTAGE_API_KEY:
        return """
⚠️ Real-time market data unavailable (no Alpha Vantage API key)

To enable real-time stock prices and market data:
1. Get free API key at https://www.alphavantage.co/support/#api-key
2. Add ALPHA_VANTAGE_API_KEY to environment variables
3. Restart the service
"""
    
    print(f"🔍 Enriching with Alpha Vantage data: {news_text[:60]}...")
    
    # Extract relevant tickers
    tickers = extract_tickers_from_news(news_text)
    
    if not tickers:
        print("⚠️ No relevant tickers found in news")
        return "\n⚠️ No relevant stock tickers identified in this news"
    
    # Get market indices
    indices = ["SPY", "QQQ", "DIA"]
    
    # Fetch data concurrently (but respect Alpha Vantage rate limits)
    tasks = []
    
    # Get quotes for relevant tickers (limit to 3 to avoid rate limits)
    for ticker in tickers[:3]:
        tasks.append(get_stock_quote(ticker))
        await asyncio.sleep(0.3)  # Small delay to respect rate limits
    
    # Get quotes for indices
    for index in indices:
        tasks.append(get_stock_quote(index))
        await asyncio.sleep(0.3)
    
    # Get news
    tasks.append(get_market_news(tickers))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Parse results
    stock_quotes = [r for r in results[:len(tickers[:3])] if isinstance(r, dict) and r]
    index_quotes = [r for r in results[len(tickers[:3]):len(tickers[:3])+3] if isinstance(r, dict) and r]
    news_items = results[-1] if isinstance(results[-1], list) else []
    
    # Build enriched context
    context_parts = []
    
    # Market indices
    if index_quotes:
        context_parts.append("\n📊 **CURRENT MARKET CONDITIONS:**")
        index_names = {"SPY": "S&P 500", "QQQ": "Nasdaq", "DIA": "Dow Jones"}
        for quote in index_quotes:
            ticker = quote.get("ticker", "")
            name = index_names.get(ticker, ticker)
            change = quote.get("change", 0)
            change_emoji = "🟢" if change > 0 else "🔴" if change < 0 else "⚪"
            context_parts.append(
                f"  {change_emoji} {name} ({ticker}): ${quote.get('price', 0):.2f} ({change:+.2f}%)"
            )
    
    # Relevant stock prices
    if stock_quotes:
        context_parts.append("\n💰 **RELEVANT STOCK PRICES (Real-Time):**")
        for quote in stock_quotes:
            change = quote.get("change", 0)
            change_emoji = "🟢" if change > 0 else "🔴" if change < 0 else "⚪"
            context_parts.append(
                f"  {change_emoji} {quote['ticker']}: ${quote['price']:.2f} "
                f"(Open: ${quote['open']:.2f}, High: ${quote['high']:.2f}, Low: ${quote['low']:.2f}) "
                f"Change: {change:+.2f}%"
            )
    
    # Market news
    if news_items:
        context_parts.append(f"\n📰 **RECENT MARKET NEWS:**")
        for i, article in enumerate(news_items[:2], 1):
            sentiment_emoji = {"Bullish": "🟢", "Bearish": "🔴", "Neutral": "⚪"}.get(article['sentiment'], "⚪")
            context_parts.append(
                f"  {i}. {sentiment_emoji} {article['title'][:80]}... ({article['source']})"
            )
    
    enriched = "\n".join(context_parts)
    
    stock_count = len(stock_quotes)
    news_count = len(news_items)
    print(f"✅ Enriched with {stock_count} real-time quotes, {news_count} news items")
    
    return enriched if enriched else "\n⚠️ No market data available for this news"


# Warning on startup if no API key
if not ALPHA_VANTAGE_API_KEY:
    print("⚠️ WARNING: ALPHA_VANTAGE_API_KEY not set. Real-time market data will be unavailable.")
    print("   Get free API key at https://www.alphavantage.co/support/#api-key")
