package pkg

import (
	"encoding/json"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type Ticket struct {
	ID          string    `json:"id"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	Category    string    `json:"category"`
	Priority    string    `json:"priority"`
	Status      string    `json:"status"`
	Response    string    `json:"response,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type Agent struct {
	Name        string
	Subject     string
	ResponseSubj string
	NC          *nats.Conn
}

func NewAgent(name, subject, responseSubj string, nc *nats.Conn) *Agent {
	return &Agent{
		Name:        name,
		Subject:     subject,
		ResponseSubj: responseSubj,
		NC:          nc,
	}
}

func (a *Agent) Start(handler func(*Ticket) (*Ticket, error)) error {
	_, err := a.NC.Subscribe(a.Subject, func(msg *nats.Msg) {
		var ticket Ticket
		if err := json.Unmarshal(msg.Data, &ticket); err != nil {
			log.Printf("[%s] Failed to parse ticket: %v", a.Name, err)
			return
		}

		log.Printf("[%s] Processing ticket %s", a.Name, ticket.ID)

		result, err := handler(&ticket)
		if err != nil {
			log.Printf("[%s] Error processing ticket %s: %v", a.Name, ticket.ID, err)
			ticket.Status = "failed"
			result = &ticket
		}

		result.UpdatedAt = time.Now()

		responseData, _ := json.Marshal(result)
		a.NC.Publish(a.ResponseSubj, responseData)
		log.Printf("[%s] Completed ticket %s", a.Name, ticket.ID)
	})

	return err
}