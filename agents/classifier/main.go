package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/08yh04-maker/lab13-variant2/pkg"
	"go.opentelemetry.io/otel/attribute"
)

var agentName = "classifier"

func classifyTicket(ctx context.Context, ticket *pkg.Ticket) (*pkg.Ticket, error) {
	_, span := pkg.GetTracer().Start(ctx, "classifyTicket")
	defer span.End()

	span.SetAttributes(
		attribute.String("ticket.id", ticket.ID),
		attribute.String("ticket.title", ticket.Title),
	)

	title := strings.ToLower(ticket.Title)
	desc := strings.ToLower(ticket.Description)

	if strings.Contains(title, "password") || strings.Contains(desc, "password") {
		ticket.Category = "account"
		ticket.Priority = "high"
	} else if strings.Contains(title, "bug") || strings.Contains(desc, "error") {
		ticket.Category = "technical"
		ticket.Priority = "medium"
	} else if strings.Contains(title, "payment") || strings.Contains(desc, "bill") {
		ticket.Category = "billing"
		ticket.Priority = "high"
	} else {
		ticket.Category = "general"
		ticket.Priority = "low"
	}

	ticket.Status = "classified"

	// Обновляем статистику в Redis
	pkg.IncrementCounter(agentName, "total_processed")
	pkg.IncrementCounter(agentName, "category:"+ticket.Category)

	span.SetAttributes(
		attribute.String("ticket.category", ticket.Category),
		attribute.String("ticket.priority", ticket.Priority),
	)

	return ticket, nil
}

func main() {
	// Инициализируем трассировку
	shutdown := pkg.InitTracer(agentName + "-agent")
	defer shutdown()

	// Инициализируем Redis
	pkg.InitRedis()

	// Загружаем состояние
	var state pkg.AgentState
	if err := state.Load(agentName); err != nil {
		log.Printf("Error loading state: %v", err)
	} else {
		log.Printf("Loaded state: processed=%d, start_time=%v",
			state.TotalProcessed, state.StartTime)
	}

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Classifier agent started with Redis state")

	_, err = nc.Subscribe("tickets.classify", func(msg *nats.Msg) {
		var ticket pkg.Ticket
		if err := json.Unmarshal(msg.Data, &ticket); err != nil {
			log.Printf("Failed to parse ticket: %v", err)
			return
		}

		ctx := context.Background()
		result, err := classifyTicket(ctx, &ticket)
		if err != nil {
			log.Printf("Error processing ticket: %v", err)
			ticket.Status = "failed"
			result = &ticket
		}

		result.UpdatedAt = time.Now()
		responseData, _ := json.Marshal(result)
		nc.Publish("tickets.classified", responseData)
		log.Printf("Completed ticket %s", ticket.ID)

		// Обновляем и сохраняем состояние
		total, _ := pkg.GetCounter(agentName, "total_processed")
		state.TotalProcessed = int(total)
		state.LastProcessed = time.Now()
		state.Save(agentName)
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}