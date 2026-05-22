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
	
	span.SetAttributes(
		attribute.String("ticket.category", ticket.Category),
		attribute.String("ticket.priority", ticket.Priority),
	)

	return ticket, nil
}

func main() {
	// Инициализируем трассировку
	shutdown := pkg.InitTracer("classifier-agent")
	defer shutdown()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Classifier agent started with tracing")

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
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}