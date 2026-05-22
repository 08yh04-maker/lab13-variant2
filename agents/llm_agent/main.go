package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"

	"github.com/nats-io/nats.go"
)

type LLMRequest struct {
	Prompt string `json:"prompt"`
}

type LLMResponse struct {
	Response string `json:"response"`
}

func callLLM(prompt string) (string, error) {
	// Для локального Ollama
	url := "http://localhost:11434/api/generate"
	payload := map[string]interface{}{
		"model":  "qwen2.5-coder:3b",
		"prompt": prompt,
		"stream": false,
	}
	
	jsonData, _ := json.Marshal(payload)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	
	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)
	
	if response, ok := result["response"].(string); ok {
		return response, nil
	}
	return "", nil
}

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("LLM Agent started")

	_, err = nc.Subscribe("tickets.llm", func(msg *nats.Msg) {
		var ticket map[string]interface{}
		json.Unmarshal(msg.Data, &ticket)
		
		prompt := "Generate a helpful response for this support ticket: " + ticket["title"].(string) + " - " + ticket["description"].(string)
		
		response, err := callLLM(prompt)
		if err != nil {
			log.Printf("LLM error: %v", err)
			response = "We're looking into your issue. Please wait."
		}
		
		ticket["llm_response"] = response
		ticket["status"] = "llm_processed"
		
		resultData, _ := json.Marshal(ticket)
		nc.Publish("tickets.llm_responded", resultData)
		log.Printf("Processed ticket with LLM")
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}