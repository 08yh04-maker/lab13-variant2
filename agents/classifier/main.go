package main

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/08yh04-maker/lab13-variant2/pkg"
)

func classifyTicket(ticket *pkg.Ticket) (*pkg.Ticket, error) {
	// Простая логика классификации
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
	return ticket, nil
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Classifier agent started")

	agent := pkg.NewAgent("Classifier", "tickets.classify", "tickets.classified", nc)
	if err := agent.Start(classifyTicket); err != nil {
		log.Fatal(err)
	}

	select {}
}