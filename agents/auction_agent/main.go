package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/08yh04-maker/lab13-variant2/pkg"
)

type AuctionRequest struct {
	AuctionID     string   `json:"auction_id"`
	TicketID      string   `json:"ticket_id"`
	Category      string   `json:"category"`
	Priority      string   `json:"priority"`
	RequiredSkills []string `json:"required_skills"`
}

type AuctionBid struct {
	AuctionID     string  `json:"auction_id"`
	AgentID       string  `json:"agent_id"`
	AgentType     string  `json:"agent_type"`
	BidPrice      float64 `json:"bid_price"`
	SkillMatch    float64 `json:"skill_match"`
	CurrentLoad   int     `json:"current_load"`
	EstimatedTime float64 `json:"estimated_time"`
}

var agentSkills = map[string][]string{
	"classifier": {"classification", "categorization"},
	"kb_search":  {"search", "knowledge_base", "documentation"},
	"responder":  {"communication", "response_generation"},
	"escalator":  {"escalation", "human_handoff"},
}

func calculateSkillMatch(required []string, agentType string) float64 {
	skills, exists := agentSkills[agentType]
	if !exists {
		return 0.5
	}
	
	matchCount := 0
	for _, req := range required {
		for _, skill := range skills {
			if req == skill {
				matchCount++
				break
			}
		}
	}
	
	if len(required) == 0 {
		return 1.0
	}
	return float64(matchCount) / float64(len(required))
}

func calculateBidPrice(agentType string, skillMatch float64) float64 {
	// Базовая цена зависит от типа агента
	basePrice := map[string]float64{
		"classifier": 10.0,
		"kb_search":  5.0,
		"responder":  8.0,
		"escalator":  15.0,
	}
	
	price := basePrice[agentType]
	// Чем выше skillMatch, тем ниже цена (конкуренция)
	price = price * (1.5 - skillMatch)
	return price
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	agentID := "classifier-1" // В реальности уникальный ID
	agentType := "classifier"

	log.Printf("Auction agent %s (%s) started", agentID, agentType)

	// Подписываемся на аукционные запросы
	_, err = nc.Subscribe("auction.request", func(msg *nats.Msg) {
		var req AuctionRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			log.Printf("Failed to parse auction request: %v", err)
			return
		}

		// Рассчитываем skill match
		skillMatch := calculateSkillMatch(req.RequiredSkills, agentType)
		
		// Текущая загрузка (имитация)
		currentLoad := rand.Intn(5)
		
		// Рассчитываем цену
		bidPrice := calculateBidPrice(agentType, skillMatch)
		
		// Оценочное время выполнения
		estimatedTime := float64(rand.Intn(3) + 1)
		
		bid := AuctionBid{
			AuctionID:     req.AuctionID,
			AgentID:       agentID,
			AgentType:     agentType,
			BidPrice:      bidPrice,
			SkillMatch:    skillMatch,
			CurrentLoad:   currentLoad,
			EstimatedTime: estimatedTime,
		}
		
		bidData, _ := json.Marshal(bid)
		nc.Publish("auction.bids", bidData)
		log.Printf("Placed bid for auction %s: price=%.2f, skill=%.2f", 
			req.AuctionID, bidPrice, skillMatch)
	})

	if err != nil {
		log.Fatal(err)
	}

	// Подписываемся на уведомления о победе
	_, err = nc.Subscribe("auction.winner."+agentID, func(msg *nats.Msg) {
		var result map[string]interface{}
		json.Unmarshal(msg.Data, &result)
		log.Printf("Won auction for ticket %v!", result["ticket_id"])
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}