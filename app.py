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
        "keywords": ["soybean", "cooking oil", "ag policy", "crop"],
        "tickers": ["ZS", "BO", "BG", "ADM"],
        "category": "commodity"
    },
    "ai_datacenter": {
        "keywords": ["AI", "datacenter", "GPU", "NVIDIA"],
        "tickers": ["NVDA", "SMCI", "DELL", "HPE"],
        "category": "sector"
    }
}


class NewsEvent(BaseModel):
    text: str
    source: str


def hash_event(text: str) -> str:
    return sha256(text.encode()).hexdigest()[:16]


@app.get("/")
async def root():
    return {"status": "ok", "message": "Market Impact API"}


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "telegram_configured": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID),
        "anthropic_configured": bool(ANTHROPIC_API_KEY)
    }


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    body = await request.json()
    return {"status": "received"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
