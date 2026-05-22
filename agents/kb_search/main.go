package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/08yh04-maker/lab13-variant2/pkg"
	"go.opentelemetry.io/otel/attribute"
)

var knowledgeBase = map[string]string{
	"account":   "To reset your password, go to login page and click 'Forgot password'.",
	"technical": "Try clearing your browser cache or restarting the application.",
	"billing":   "Please contact our billing team at billing@support.com",
	"general":   "We'll get back to you within 24 hours.",
}

func searchKB(ctx context.Context, ticket *pkg.Ticket) (*pkg.Ticket, error) {
	_, span := pkg.GetTracer().Start(ctx, "kb_search")
	defer span.End()

	span.SetAttributes(attribute.String("ticket.category", ticket.Category))

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
	shutdown := pkg.InitTracer("kb-search-agent")
	defer shutdown()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Knowledge Base Search agent started with tracing")

	_, err = nc.Subscribe("tickets.classified", func(msg *nats.Msg) {
		var ticket pkg.Ticket
		if err := json.Unmarshal(msg.Data, &ticket); err != nil {
			log.Printf("Failed to parse ticket: %v", err)
			return
		}

		ctx := context.Background()
		result, err := searchKB(ctx, &ticket)
		if err != nil {
			log.Printf("Error: %v", err)
			result = &ticket
		}

		responseData, _ := json.Marshal(result)
		nc.Publish("tickets.kb_searched", responseData)
		log.Printf("KB search completed for ticket %s", ticket.ID)
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}