package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/08yh04-maker/lab13-variant2/pkg"
)

func generateResponse(ctx context.Context, ticket *pkg.Ticket) (*pkg.Ticket, error) {
	_, span := pkg.GetTracer().Start(ctx, "responder")
	defer span.End()

	if ticket.Status == "kb_found" && ticket.Response != "" {
		ticket.Status = "responded"
	} else if ticket.Status == "escalate" {
		ticket.Response = "Your ticket has been escalated to a human agent. Reference ID: " + ticket.ID
		ticket.Status = "escalated"
	} else {
		ticket.Response = "Thank you for your ticket. We're looking into it."
		ticket.Status = "responded"
	}

	return ticket, nil
}

func main() {
	shutdown := pkg.InitTracer("responder-agent")
	defer shutdown()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Responder agent started with tracing")

	_, err = nc.Subscribe("tickets.kb_searched", func(msg *nats.Msg) {
		var ticket pkg.Ticket
		if err := json.Unmarshal(msg.Data, &ticket); err != nil {
			log.Printf("Failed to parse ticket: %v", err)
			return
		}

		ctx := context.Background()
		result, err := generateResponse(ctx, &ticket)
		if err != nil {
			log.Printf("Error: %v", err)
			result = &ticket
		}

		responseData, _ := json.Marshal(result)
		nc.Publish("tickets.responded", responseData)
		log.Printf("Response generated for ticket %s", ticket.ID)
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}