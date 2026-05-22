package main

import (
	"log"

	"github.com/nats-io/nats.go"
	"github.com/08yh04-maker/lab13-variant2/pkg"
)

func escalateTicket(ticket *pkg.Ticket) (*pkg.Ticket, error) {
	if ticket.Status == "escalated" {
		log.Printf("[Escalator] Ticket %s escalated to human support", ticket.ID)
		ticket.Status = "human_assigned"
	}

	return ticket, nil
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Escalator agent started")

	agent := pkg.NewAgent("Escalator", "tickets.escalated", "tickets.final", nc)
	if err := agent.Start(escalateTicket); err != nil {
		log.Fatal(err)
	}

	select {}
}