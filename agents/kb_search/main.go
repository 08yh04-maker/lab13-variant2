package main

import (
	"log"

	"github.com/nats-io/nats.go"
	"github.com/08yh04-maker/lab13-variant2/pkg"
)

var knowledgeBase = map[string]string{
	"account": "To reset your password, go to login page and click 'Forgot password'.",
	"technical": "Try clearing your browser cache or restarting the application.",
	"billing": "Please contact our billing team at billing@support.com",
	"general": "We'll get back to you within 24 hours.",
}

func searchKB(ticket *pkg.Ticket) (*pkg.Ticket, error) {
	if answer, exists := knowledgeBase[ticket.Category]; exists {
		ticket.Response = answer
		ticket.Status = "kb_found"
	} else {
		ticket.Response = "No matching solution found. Escalating to human agent."
		ticket.Status = "escalate"
	}

	return ticket, nil
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Knowledge Base Search agent started")

	agent := pkg.NewAgent("KBSearch", "tickets.classified", "tickets.kb_searched", nc)
	if err := agent.Start(searchKB); err != nil {
		log.Fatal(err)
	}

	select {}
}