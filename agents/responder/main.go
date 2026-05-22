package main

import (
	"log"

	"github.com/nats-io/nats.go"
	"github.com/08yh04-maker/lab13-variant2/pkg"
)

func generateResponse(ticket *pkg.Ticket) (*pkg.Ticket, error) {
	if ticket.Status == "kb_found" && ticket.Response != "" {
		// Уже есть ответ из базы знаний
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
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Responder agent started")

	agent := pkg.NewAgent("Responder", "tickets.kb_searched", "tickets.responded", nc)
	if err := agent.Start(generateResponse); err != nil {
		log.Fatal(err)
	}

	select {}
}