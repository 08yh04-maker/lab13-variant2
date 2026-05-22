import asyncio
import json
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime

import nats

logger = logging.getLogger("auction")


@dataclass
class AgentBid:
    agent_id: str
    agent_type: str
    bid_price: float  # стоимость (чем ниже, тем лучше)
    skill_match: float  # 0-1 насколько подходит
    current_load: int  # текущая загрузка
    estimated_time: float  # оценочное время выполнения (сек)


class AuctionHouse:
    def __init__(self):
        self.nc = None
        self.pending_auctions: Dict[str, asyncio.Future] = {}
        self.auction_timeout = 5  # секунд
        self.bids: Dict[str, List[AgentBid]] = {}
        
    async def connect(self):
        self.nc = await nats.connect("nats://localhost:4222")
        
        # Подписываемся на ответы от агентов
        await self.nc.subscribe("auction.bids", cb=self.on_bid_received)
        logger.info("Auction house connected to NATS")
    
    async def on_bid_received(self, msg):
        data = json.loads(msg.data.decode())
        auction_id = data.get("auction_id")
        bid = AgentBid(
            agent_id=data["agent_id"],
            agent_type=data["agent_type"],
            bid_price=data["bid_price"],
            skill_match=data["skill_match"],
            current_load=data["current_load"],
            estimated_time=data["estimated_time"]
        )
        
        if auction_id not in self.bids:
            self.bids[auction_id] = []
        self.bids[auction_id].append(bid)
        logger.info(f"Received bid from {bid.agent_id} for auction {auction_id}: price={bid.bid_price}, skill={bid.skill_match}")
    
    async def start_auction(self, ticket_id: str, ticket_category: str, ticket_priority: str) -> Optional[AgentBid]:
        """Начинает аукцион для задачи и возвращает лучшую ставку"""
        auction_id = f"auction_{ticket_id}"
        self.bids[auction_id] = []
        
        # Создаём запрос на аукцион
        auction_request = {
            "auction_id": auction_id,
            "ticket_id": ticket_id,
            "category": ticket_category,
            "priority": ticket_priority,
            "required_skills": self._get_required_skills(ticket_category)
        }
        
        # Отправляем запрос всем агентам (broadcast)
        await self.nc.publish("auction.request", json.dumps(auction_request).encode())
        logger.info(f"Started auction {auction_id} for ticket {ticket_id}")
        
        # Ждём ставки в течение auction_timeout секунд
        await asyncio.sleep(self.auction_timeout)
        
        # Выбираем лучшую ставку
        best_bid = self._select_best_bid(auction_id)
        
        if best_bid:
            # Уведомляем победителя
            await self.nc.publish(f"auction.winner.{best_bid.agent_id}", json.dumps({
                "auction_id": auction_id,
                "ticket_id": ticket_id,
                "won": True
            }).encode())
            logger.info(f"Winner for auction {auction_id}: {best_bid.agent_id}")
        else:
            logger.warning(f"No bids received for auction {auction_id}")
        
        # Очищаем
        if auction_id in self.bids:
            del self.bids[auction_id]
        
        return best_bid
    
    def _get_required_skills(self, category: str) -> List[str]:
        skills_map = {
            "account": ["password_reset", "account_management"],
            "technical": ["debugging", "error_analysis", "system_logs"],
            "billing": ["invoice", "payment_processing", "refund"],
            "general": ["communication", "customer_service"]
        }
        return skills_map.get(category, ["general_support"])
    
    def _select_best_bid(self, auction_id: str) -> Optional[AgentBid]:
        bids = self.bids.get(auction_id, [])
        if not bids:
            return None
        
        # Комбинированная оценка: цена * (1 + load/10) / skill_match
        # Чем меньше, тем лучше
        def score(bid: AgentBid) -> float:
            load_penalty = 1 + (bid.current_load / 10)
            skill_bonus = max(0.5, bid.skill_match)  # минимум 0.5
            return (bid.bid_price * load_penalty) / skill_bonus
        
        best = min(bids, key=score)
        logger.info(f"Selected bid from {best.agent_id} with score {score(best):.2f}")
        return best


# Singleton
auction_house = AuctionHouse()