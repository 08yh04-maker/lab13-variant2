import asyncio
import json
import logging
import httpx
from typing import Optional

import nats
from nats.aio.client import Client as NATS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("llm-agent")

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL_NAME = "qwen2.5-coder:3b"


async def call_llm(prompt: str) -> str:
    """Вызов локальной LLM через Ollama"""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                OLLAMA_URL,
                json={
                    "model": MODEL_NAME,
                    "prompt": prompt,
                    "stream": False
                }
            )
            if response.status_code == 200:
                data = response.json()
                return data.get("response", "No response from LLM")
            else:
                logger.error(f"Ollama error: {response.status_code}")
                return "LLM service temporarily unavailable"
    except Exception as e:
        logger.error(f"LLM call failed: {e}")
        return "Error contacting AI service"


async def main():
    nc = await nats.connect("nats://localhost:4222")
    logger.info("LLM Agent (Python) started")

    async def handle_ticket(msg):
        data = json.loads(msg.data.decode())
        ticket_id = data.get("id")
        title = data.get("title", "")
        description = data.get("description", "")
        
        logger.info(f"Processing ticket {ticket_id} with LLM")
        
        prompt = f"""You are a tech support assistant. 
        Generate a helpful response for this support ticket:
        Title: {title}
        Description: {description}
        Keep response short and professional."""
        
        response_text = await call_llm(prompt)
        
        data["llm_response"] = response_text
        data["status"] = "llm_processed"
        
        await nc.publish("tickets.llm_responded", json.dumps(data).encode())
        logger.info(f"Ticket {ticket_id} processed by LLM")

    await nc.subscribe("tickets.llm", cb=handle_ticket)
    
    # Держим агента активным
    while True:
        await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(main())