package web

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
	"linkage-gateway/internal/store"
)

type Hub struct {
	mu        sync.Mutex
	clients   map[*client]struct{}
	queueSize int
	location  *time.Location
}

type client struct {
	ctx    context.Context
	cancel context.CancelFunc
	send   chan []byte
}

type linkageMessage struct {
	Type          string           `json:"type"`
	SchemaVersion int              `json:"schemaVersion"`
	EventID       string           `json:"eventId"`
	Sequence      int64            `json:"sequence"`
	PointKey      string           `json:"pointKey"`
	StationNo     uint16           `json:"stationNo"`
	MonitorIndex  uint16           `json:"monitorIndex"`
	DeviceName    string           `json:"deviceName"`
	EventType     string           `json:"eventType"`
	Property      string           `json:"property"`
	Value         string           `json:"value"`
	EventTime     string           `json:"eventTime"`
	ReceivedAt    string           `json:"receivedAt"`
	MatchStatus   string           `json:"matchStatus"`
	Channels      []linkageChannel `json:"channels"`
}

type linkageChannel struct {
	DeviceID  string `json:"deviceId"`
	ChannelID string `json:"channelId"`
	Name      string `json:"name"`
}

func NewHub(queueSize int, location *time.Location) *Hub {
	if queueSize < 1 {
		queueSize = 64
	}
	if location == nil {
		location = time.UTC
	}
	return &Hub{
		clients:   make(map[*client]struct{}),
		queueSize: queueSize,
		location:  location,
	}
}

func (hub *Hub) register(ctx context.Context, cancel context.CancelFunc) *client {
	newClient := &client{ctx: ctx, cancel: cancel, send: make(chan []byte, hub.queueSize)}
	hub.mu.Lock()
	hub.clients[newClient] = struct{}{}
	hub.mu.Unlock()
	return newClient
}

func (hub *Hub) remove(target *client) {
	hub.mu.Lock()
	delete(hub.clients, target)
	hub.mu.Unlock()
}

func (hub *Hub) Publish(event store.Event) int {
	message := linkageMessage{
		Type:          "linkage_event",
		SchemaVersion: 1,
		EventID:       event.ID,
		Sequence:      event.Sequence,
		PointKey:      event.PointKey,
		StationNo:     event.StationNo,
		MonitorIndex:  event.MonitorIndex,
		DeviceName:    event.DeviceName,
		EventType:     event.EventType,
		Property:      event.Property,
		Value:         event.Value,
		EventTime:     event.EventTime.In(hub.location).Format(time.RFC3339Nano),
		ReceivedAt:    event.ReceivedAt.In(hub.location).Format(time.RFC3339Nano),
		MatchStatus:   event.MatchStatus,
		Channels:      make([]linkageChannel, 0, len(event.Channels)),
	}
	for _, channel := range event.Channels {
		message.Channels = append(message.Channels, linkageChannel{
			DeviceID:  channel.WVPDeviceID,
			ChannelID: channel.WVPChannelID,
			Name:      channel.ChannelName,
		})
	}
	payload, err := json.Marshal(message)
	if err != nil {
		return 0
	}

	published := 0
	hub.mu.Lock()
	for target := range hub.clients {
		select {
		case <-target.ctx.Done():
			delete(hub.clients, target)
		case target.send <- payload:
			published++
		default:
			delete(hub.clients, target)
			target.cancel()
		}
	}
	hub.mu.Unlock()
	return published
}

func (hub *Hub) ClientCount() int {
	hub.mu.Lock()
	defer hub.mu.Unlock()
	return len(hub.clients)
}

func (hub *Hub) Close() {
	hub.mu.Lock()
	for target := range hub.clients {
		delete(hub.clients, target)
		target.cancel()
	}
	hub.mu.Unlock()
}

func (hub *Hub) handleConnection(writer http.ResponseWriter, request *http.Request, origins []string) {
	connection, err := websocket.Accept(writer, request, &websocket.AcceptOptions{OriginPatterns: origins})
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	target := hub.register(ctx, cancel)
	defer func() {
		hub.remove(target)
		cancel()
		connection.CloseNow()
	}()
	connection.SetReadLimit(1024)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			if _, _, err := connection.Read(ctx); err != nil {
				return
			}
		}
	}()
	for {
		select {
		case <-readerDone:
			return
		case <-ctx.Done():
			return
		case payload := <-target.send:
			writeCtx, writeCancel := context.WithTimeout(ctx, 5*time.Second)
			err := connection.Write(writeCtx, websocket.MessageText, payload)
			writeCancel()
			if err != nil {
				return
			}
		}
	}
}
