import asyncio
import subprocess
import logging
from typing import Dict, List

import nats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - scaler - %(levelname)s - %(message)s'
)
logger = logging.getLogger("scaler")


class AgentScaler:
    def __init__(self):
        self.nc = None
        self.agent_processes: Dict[str, List] = {
            "classifier": [],
            "kb_search": [],
            "responder": [],
            "escalator": []
        }
        self.target_count = 1
        self.check_interval = 15
        self.request_count = 0
        
    async def connect(self):
        self.nc = await nats.connect("nats://localhost:4222")
        logger.info("Connected to NATS")
        await self.nc.subscribe("tickets.classify", cb=self.on_ticket_received)
    
    async def on_ticket_received(self, msg):
        self.request_count += 1
        logger.info(f"Ticket received. Total: {self.request_count}")
        
        if self.request_count > 10 and self.target_count < 3:
            self.target_count = 3
            await self.scale_agents()
        elif self.request_count > 5 and self.target_count < 2:
            self.target_count = 2
            await self.scale_agents()
    
    async def scale_agents(self):
        logger.info(f"Scaling agents to {self.target_count} instances")
        
        for agent_name in ["classifier", "kb_search", "responder", "escalator"]:
            current = len(self.agent_processes[agent_name])
            
            if current < self.target_count:
                for i in range(current, self.target_count):
                    process = subprocess.Popen(
                        ["go", "run", f"agents/{agent_name}/main.go"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    self.agent_processes[agent_name].append(process)
                    logger.info(f"Started {agent_name} agent instance {i+1}")
    
    async def run(self):
        await self.connect()
        logger.info("Agent Scaler started")
        await self.scale_agents()
        
        while True:
            await asyncio.sleep(self.check_interval)
            logger.info(f"Current load: {self.request_count} tickets, scaling level: {self.target_count}")


if __name__ == "__main__":
    scaler = AgentScaler()
    asyncio.run(scaler.run())