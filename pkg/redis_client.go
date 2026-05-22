package pkg

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

var RedisClient *redis.Client

func InitRedis() {
	RedisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	ctx := context.Background()
	if err := RedisClient.Ping(ctx).Err(); err != nil {
		log.Printf("Warning: Redis connection failed: %v", err)
	} else {
		log.Println("Redis connected successfully")
	}
}

type AgentState struct {
	TotalProcessed int            `json:"total_processed"`
	CategoryCount  map[string]int `json:"category_count"`
	LastProcessed  time.Time      `json:"last_processed"`
	StartTime      time.Time      `json:"start_time"`
}

func (s *AgentState) Save(agentName string) error {
	ctx := context.Background()
	data, err := json.Marshal(s)
	if err != nil {
		return err
	}
	return RedisClient.Set(ctx, "agent:"+agentName+":state", data, 0).Err()
}

func (s *AgentState) Load(agentName string) error {
	ctx := context.Background()
	data, err := RedisClient.Get(ctx, "agent:"+agentName+":state").Bytes()
	if err != nil {
		if err == redis.Nil {
			// Состояние не найдено, инициализируем новое
			s.TotalProcessed = 0
			s.CategoryCount = make(map[string]int)
			s.StartTime = time.Now()
			s.LastProcessed = time.Now()
			return nil
		}
		return err
	}
	return json.Unmarshal(data, s)
}

func IncrementCounter(agentName, key string) error {
	ctx := context.Background()
	return RedisClient.Incr(ctx, "agent:"+agentName+":counter:"+key).Err()
}

func GetCounter(agentName, key string) (int64, error) {
	ctx := context.Background()
	return RedisClient.Get(ctx, "agent:"+agentName+":counter:"+key).Int64()
}