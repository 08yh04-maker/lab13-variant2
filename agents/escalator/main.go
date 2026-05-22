package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/08yh04-maker/lab13-variant2/pkg"
)

func escalateTicket(ctx context.Context, ticket *pkg.Ticket) (*pkg.Ticket, error) {
	_, span := pkg.GetTracer().Start(ctx, "escalator")
	defer span.End()

	if ticket.Status == "escalated" {
		log.Printf("[Escalator] Ticket %s escalated to human support", ticket.ID)
		ticket.Status = "human_assigned"
	}

	return ticket, nil
}

func main() {
	shutdown := pkg.InitTracer("escalator-agent")
	defer shutdown()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Escalator agent started with tracing")

	_, err = nc.Subscribe("tickets.escalated", func(msg *nats.Msg) {
		var ticket pkg.Ticket
		if err := json.Unmarshal(msg.Data, &ticket); err != nil {
			log.Printf("Failed to parse ticket: %v", err)
			return
		}

		ctx := context.Background()
		result, err := escalateTicket(ctx, &ticket)
		if err != nil {
			log.Printf("Error: %v", err)
			result = &ticket
		}

		responseData, _ := json.Marshal(result)
		nc.Publish("tickets.final", responseData)
		log.Printf("Ticket %s finalized", ticket.ID)
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}