import asyncio
import json
import uuid
import logging
from .tracing import init_tracing
from datetime import datetime
from typing import Dict, Optional
from contextlib import asynccontextmanager

import nats
from nats.aio.client import Client as NATS
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from .auction import auction_house

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("orchestrator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("orchestrator")


# Pydantic модели
class TicketRequest(BaseModel):
    title: str
    description: str


class TicketStatus(BaseModel):
    id: str
    title: str
    status: str
    category: str
    priority: str
    response: Optional[str] = None


class Orchestrator:
    def __init__(self):
        self.nc: Optional[NATS] = None
        self.results: Dict[str, asyncio.Future] = {}
        self.tickets: Dict[str, dict] = {}
        self.task_counter = 0
        self.processed_counter = 0

    async def connect(self):
        self.nc = await nats.connect("nats://localhost:4222")
        logger.info("Connected to NATS")
        await self.start_listener()

    async def start_listener(self):
        await self.nc.subscribe("tickets.final", cb=self.on_final_result)
        logger.info("Listening for final results")

    async def on_final_result(self, msg):
        data = json.loads(msg.data.decode())
        ticket_id = data.get("id")
        if ticket_id in self.results:
            self.results[ticket_id].set_result(data)
            self.processed_counter += 1
            logger.info(f"Task {ticket_id} completed. Total processed: {self.processed_counter}")

   async def process_ticket_pipeline(self, title: str, description: str, timeout: int = 60) -> dict:
    from opentelemetry import trace
    tracer = trace.get_tracer(__name__)
    
    with tracer.start_as_current_span("process_ticket_pipeline") as span:
        span.set_attribute("ticket.title", title)
        
        ticket_id = str(uuid.uuid4())
        
        # Аукцион: выбираем лучшего агента для обработки
        # Сначала определяем категорию (предварительно)
        temp_category = self._guess_category(title, description)
        
        # Запускаем аукцион
        winner = await auction_house.start_auction(ticket_id, temp_category, "medium")
        
        if winner:
            logger.info(f"Ticket {ticket_id} assigned to {winner.agent_id} via auction")
            span.set_attribute("auction.winner", winner.agent_id)
            span.set_attribute("auction.price", winner.bid_price)
        
        # Продолжаем обычный pipeline...
        ticket = {
            "id": ticket_id,
            "title": title,
            "description": description,
            "status": "new",
            "created_at": datetime.now().isoformat()
        }
       
def _guess_category(self, title: str, description: str) -> str:
    text = (title + " " + description).lower()
    if "password" in text or "login" in text:
        return "account"
    elif "bug" in text or "error" in text or "crash" in text:
        return "technical"
    elif "payment" in text or "bill" in text or "invoice" in text:
        return "billing"
    return "general"

        self.tickets[ticket_id] = ticket
        future = asyncio.Future()
        self.results[ticket_id] = future

        logger.info(f"Starting pipeline for ticket {ticket_id}")

        # Шаг 1: Отправляем на классификацию
        await self.nc.publish("tickets.classify", json.dumps(ticket).encode())
        logger.info(f"Ticket {ticket_id} sent to classifier")

        # Шаг 2: Ждём результат после классификации
        try:
            result = await asyncio.wait_for(future, timeout)
            return result
        except asyncio.TimeoutError:
            logger.error(f"Pipeline timeout for ticket {ticket_id}")
            del self.results[ticket_id]
            raise TimeoutError(f"Ticket {ticket_id} processing timeout")

    async def get_ticket_status(self, ticket_id: str) -> Optional[dict]:
        if ticket_id in self.tickets:
            return self.tickets[ticket_id]
        return None

    def get_stats(self) -> dict:
        return {
            "total_processed": self.processed_counter,
            "pending_tasks": len(self.results),
            "total_tickets": len(self.tickets)
        }


# FastAPI приложение
orchestrator = Orchestrator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await orchestrator.connect()
    yield
    await orchestrator.nc.drain()


app = FastAPI(
    title="Tech Support Multi-Agent System",
    description="Orchestrator for ticket processing pipeline",
    version="1.0.0",
    lifespan=lifespan
)


tracer = init_tracing(app, "orchestrator")

@app.post("/ticket", response_model=TicketStatus)
async def create_ticket(request: TicketRequest):
     from opentelemetry import trace
    tracer = trace.get_tracer(__name__)
    
    with tracer.start_as_current_span("process_ticket_pipeline") as span:
        span.set_attribute("ticket.title", title)
        
        ticket_id = str(uuid.uuid4())
try:
        result = await orchestrator.process_ticket_pipeline(
            title=request.title,
            description=request.description
        )
        return TicketStatus(
            id=result["id"],
            title=result["title"],
            status=result["status"],
            category=result.get("category", "unknown"),
            priority=result.get("priority", "low"),
            response=result.get("response")
        )
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Ticket processing timeout")
    except Exception as e:
        logger.error(f"Error processing ticket: {e}")
        raise HTTPException(status_code=500, detail=str(e))

span.set_attribute("ticket.id", ticket_id)


@app.get("/ticket/{ticket_id}/status", response_model=TicketStatus)
async def get_status(ticket_id: str):
    ticket = await orchestrator.get_ticket_status(ticket_id)
    if not ticket:
        raise HTTPException(status_code=404, detail="Ticket not found")
    return TicketStatus(
        id=ticket["id"],
        title=ticket["title"],
        status=ticket["status"],
        category=ticket.get("category", "unknown"),
        priority=ticket.get("priority", "low"),
        response=ticket.get("response")
    )


@app.get("/stats")
async def get_stats():
    return orchestrator.get_stats()


@app.get("/health")
async def health():
    return {"status": "ok", "service": "orchestrator"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)