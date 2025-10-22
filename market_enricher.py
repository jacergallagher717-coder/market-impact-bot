"""
Market Data Enrichment Layer
Fetches real-time data to provide context to Claude AI
"""

import httpx
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import re


class MarketDataEnricher:
    """Enriches news with real-time market data and recent events"""
    
    def __init__(self):
        self.yfinance_base = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=1d&range=5d"
        self.quote_url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols={tickers}"
        self.news_url = "https://query1.finance.yahoo.com/v1/finance/search?q={query}&newsCount=5"
        
    async def extract_tickers(self, text: str) -> List[str]:
        """Extract potential ticker symbols from news text"""
        # Common company names to ticker mappings
        company_map = {
            'apple': 'AAPL', 'microsoft': 'MSFT', 'amazon': 'AMZN', 'google': 'GOOGL',
            'meta': 'META', 'facebook': 'META', 'tesla': 'TSLA', 'nvidia': 'NVDA',
            'netflix': 'NFLX', 'jpmorgan': 'JPM', 'goldman': 'GS', 'morgan stanley': 'MS',
            'bank of america': 'BAC', 'wells fargo': 'WFC', 'citigroup': 'C',
            'walmart': 'WMT', 'target': 'TGT', 'costco': 'COST', 'home depot': 'HD',
            'boeing': 'BA', 'lockheed': 'LMT', 'raytheon': 'RTX',
            'exxon': 'XOM', 'chevron': 'CVX', 'conocophillips': 'COP',
            'pfizer': 'PFE', 'merck': 'MRK', 'johnson': 'JNJ', 'abbvie': 'ABBV',
            'intel': 'INTC', 'amd': 'AMD', 'qualcomm': 'QCOM', 'broadcom': 'AVGO',
            'tsmc': 'TSM', 'asml': 'ASML', 'lam research': 'LRCX',
            'salesforce': 'CRM', 'oracle': 'ORCL', 'adobe': 'ADBE',
            'visa': 'V', 'mastercard': 'MA', 'paypal': 'PYPL',
            'uber': 'UBER', 'lyft': 'LYFT', 'airbnb': 'ABNB',
            'fed': 'SPY', 'federal reserve': 'SPY', 's&p': 'SPY', 'dow': 'DIA', 'nasdaq': 'QQQ'
        }
        
        text_lower = text.lower()
        found_tickers = set()
        
        # Check for company name mentions
        for name, ticker in company_map.items():
            if name in text_lower:
                found_tickers.add(ticker)
        
        # Extract explicit ticker symbols ($AAPL or AAPL)
        ticker_pattern = r'\b[A-Z]{1,5}\b'
        explicit_tickers = re.findall(ticker_pattern, text)
        found_tickers.update(explicit_tickers[:5])  # Limit to 5
        
        return list(found_tickers)[:10]  # Max 10 tickers
    
    async def get_stock_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch real-time stock data for a ticker"""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(self.quote_url.format(tickers=ticker))
                data = response.json()
                
                if 'quoteResponse' in data and data['quoteResponse']['result']:
                    quote = data['quoteResponse']['result'][0]
                    
                    return {
                        'ticker': ticker,
                        'price': quote.get('regularMarketPrice', 0),
                        'change': quote.get('regularMarketChange', 0),
                        'change_percent': quote.get('regularMarketChangePercent', 0),
                        'volume': quote.get('regularMarketVolume', 0),
                        'market_cap': quote.get('marketCap', 0),
                        'pe_ratio': quote.get('trailingPE'),
                        'fifty_two_week_high': quote.get('fiftyTwoWeekHigh'),
                        'fifty_two_week_low': quote.get('fiftyTwoWeekLow'),
                        'avg_volume': quote.get('averageDailyVolume3Month')
                    }
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            return None
    
    async def get_recent_news(self, query: str, limit: int = 3) -> List[Dict[str, str]]:
        """Fetch recent news headlines related to query"""
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(self.news_url.format(query=query))
                data = response.json()
                
                news_items = []
                if 'news' in data:
                    for item in data['news'][:limit]:
                        news_items.append({
                            'title': item.get('title', ''),
                            'publisher': item.get('publisher', ''),
                            'published': item.get('providerPublishTime', 0)
                        })
                
                return news_items
        except Exception as e:
            print(f"Error fetching news: {e}")
            return []
    
    async def get_market_context(self) -> Dict[str, Any]:
        """Get current market context (SPY, VIX, sector performance)"""
        try:
            indices = ['SPY', 'QQQ', 'DIA', 'IWM', '^VIX']
            context = {}
            
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(self.quote_url.format(tickers=','.join(indices)))
                data = response.json()
                
                if 'quoteResponse' in data:
                    for quote in data['quoteResponse']['result']:
                        symbol = quote.get('symbol', '')
                        context[symbol] = {
                            'price': quote.get('regularMarketPrice', 0),
                            'change_percent': quote.get('regularMarketChangePercent', 0)
                        }
            
            return context
        except Exception as e:
            print(f"Error fetching market context: {e}")
            return {}
    
    async def enrich_news(self, headline: str) -> Dict[str, Any]:
        """
        Main enrichment function - gathers all context for a news headline
        Returns enriched data to include in Claude's prompt
        """
        print(f"🔍 Enriching: {headline[:60]}...")
        
        # Run all enrichment tasks in parallel
        tickers = await self.extract_tickers(headline)
        
        # Fetch data for all tickers + market context
        tasks = [
            self.get_market_context(),
            *[self.get_stock_data(ticker) for ticker in tickers[:5]],  # Limit to 5 tickers
            self.get_recent_news(headline.split()[0] if headline else "market", limit=3)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        market_context = results[0] if not isinstance(results[0], Exception) else {}
        stock_data = [r for r in results[1:-1] if r and not isinstance(r, Exception)]
        recent_news = results[-1] if not isinstance(results[-1], Exception) else []
        
        # Build enriched context
        enriched = {
            'timestamp': datetime.now().isoformat(),
            'market_context': market_context,
            'mentioned_stocks': stock_data,
            'recent_related_news': recent_news,
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M EST')
        }
        
        print(f"✅ Enriched with {len(stock_data)} stocks, {len(recent_news)} news items")
        
        return enriched
    
    def format_enrichment_for_prompt(self, enriched: Dict[str, Any]) -> str:
        """Format enriched data into a readable prompt section"""
        lines = [
            "=== REAL-TIME MARKET DATA ===",
            f"Analysis Date: {enriched['analysis_date']}",
            ""
        ]
        
        # Market context
        if enriched['market_context']:
            lines.append("Current Market Levels:")
            for ticker, data in enriched['market_context'].items():
                change_str = f"{data['change_percent']:+.2f}%" if data['change_percent'] else "N/A"
                lines.append(f"  {ticker}: ${data['price']:.2f} ({change_str})")
            lines.append("")
        
        # Stock data
        if enriched['mentioned_stocks']:
            lines.append("Mentioned Stocks (Real-Time Data):")
            for stock in enriched['mentioned_stocks']:
                lines.append(f"  {stock['ticker']}:")
                lines.append(f"    Price: ${stock['price']:.2f} ({stock['change_percent']:+.2f}%)")
                if stock['market_cap']:
                    market_cap_b = stock['market_cap'] / 1e9
                    lines.append(f"    Market Cap: ${market_cap_b:.1f}B")
                if stock['pe_ratio']:
                    lines.append(f"    P/E Ratio: {stock['pe_ratio']:.1f}")
                if stock['fifty_two_week_high'] and stock['fifty_two_week_low']:
                    lines.append(f"    52W Range: ${stock['fifty_two_week_low']:.2f} - ${stock['fifty_two_week_high']:.2f}")
            lines.append("")
        
        # Recent news
        if enriched['recent_related_news']:
            lines.append("Recent Related News (Past 7 Days):")
            for news in enriched['recent_related_news']:
                if news['title']:
                    lines.append(f"  • {news['title']}")
            lines.append("")
        
        lines.append("=== END MARKET DATA ===")
        lines.append("")
        
        return "\n".join(lines)


# Global enricher instance
enricher = MarketDataEnricher()


async def get_enriched_context(headline: str) -> str:
    """
    Get enriched market context for a headline
    This gets called before sending to Claude
    """
    try:
        enriched = await enricher.enrich_news(headline)
        return enricher.format_enrichment_for_prompt(enriched)
    except Exception as e:
        print(f"Enrichment error: {e}")
        return ""
