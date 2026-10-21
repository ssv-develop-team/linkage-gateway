package web

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"linkage-gateway/internal/config"
	"linkage-gateway/internal/protocol"
	"linkage-gateway/internal/store"
)

func TestHealthReadinessAndLinkageREST(t *testing.T) {
	ctx := context.Background()
	server, storage := newTestServer(t)
	receivedAt := time.Date(2026, time.October, 19, 15, 42, 40, 382000000, time.FixedZone("CST", 8*60*60))
	point := protocol.DevicePoint{
		StationNo:    1,
		MonitorIndex: 1,
		DeviceName:   "测试遥信点",
		DeviceType:   "遥信",
		PhysicalID:   "physical-1",
		Linkage:      true,
	}
	if _, err := storage.ImportPoints(ctx, point.StationNo, []protocol.DevicePoint{point}, "point-hash", receivedAt); err != nil {
		t.Fatalf("ImportPoints() error = %v", err)
	}

	healthResponse := requestHandler(t, server.Handler(), http.MethodGet, "/healthz", "")
	if healthResponse.Code != http.StatusOK || !strings.Contains(healthResponse.Body.String(), `"status":"ok"`) {
		t.Fatalf("GET /healthz = %d %s", healthResponse.Code, healthResponse.Body.String())
	}
	readyResponse := requestHandler(t, server.Handler(), http.MethodGet, "/readyz", "")
	if readyResponse.Code != http.StatusOK || !strings.Contains(readyResponse.Body.String(), `"status":"ready"`) {
		t.Fatalf("GET /readyz = %d %s", readyResponse.Code, readyResponse.Body.String())
	}

	pointsResponse := requestHandler(t, server.Handler(), http.MethodGet, "/api/v1/linkage/points", "")
	var points []map[string]any
	if pointsResponse.Code != http.StatusOK || json.Unmarshal(pointsResponse.Body.Bytes(), &points) != nil || len(points) != 1 {
		t.Fatalf("GET points = %d %s", pointsResponse.Code, pointsResponse.Body.String())
	}
	if points[0]["pointKey"] != "1_1" || points[0]["linkageEnabled"] != true {
		t.Fatalf("unexpected point response: %+v", points[0])
	}

	bindingBody := `{"bindings":[{"deviceId":"wvp-device-1","channelId":"wvp-channel-1","name":"摄像机一","enabled":true,"displayOrder":0}]}`
	bindingResponse := requestHandler(t, server.Handler(), http.MethodPut, "/api/v1/linkage/bindings/1_1", bindingBody)
	var bindings []store.Binding
	if bindingResponse.Code != http.StatusOK || json.Unmarshal(bindingResponse.Body.Bytes(), &bindings) != nil || len(bindings) != 1 {
		t.Fatalf("PUT binding = %d %s", bindingResponse.Code, bindingResponse.Body.String())
	}
	if bindings[0].WVPDeviceID != "wvp-device-1" || bindings[0].WVPChannelID != "wvp-channel-1" {
		t.Fatalf("unexpected binding response: %+v", bindings[0])
	}

	allBindingsResponse := requestHandler(t, server.Handler(), http.MethodGet, "/api/v1/linkage/bindings", "")
	var allBindings []store.Binding
	if allBindingsResponse.Code != http.StatusOK || json.Unmarshal(allBindingsResponse.Body.Bytes(), &allBindings) != nil || len(allBindings) != 1 {
		t.Fatalf("GET all bindings = %d %s", allBindingsResponse.Code, allBindingsResponse.Body.String())
	}
	if allBindings[0].PointKey != "1_1" || allBindings[0].WVPChannelID != "wvp-channel-1" {
		t.Fatalf("unexpected all bindings response: %+v", allBindings)
	}

	event, duplicate, err := storage.InsertEvent(ctx, store.Event{
		PointKey:        "1_1",
		StationNo:       1,
		MonitorIndex:    1,
		DeviceName:      point.DeviceName,
		EventType:       "遥信",
		EventTypeCode:   1,
		Property:        "变位",
		Value:           "合位",
		EventTime:       receivedAt,
		ReceivedAt:      receivedAt,
		RawFrameHash:    "event-hash",
		MatchStatus:     "matched",
		BroadcastStatus: "published",
		Channels:        bindings,
	}, 0)
	if err != nil || duplicate {
		t.Fatalf("InsertEvent() duplicate=%t error=%v", duplicate, err)
	}
	eventsResponse := requestHandler(t, server.Handler(), http.MethodGet, "/api/v1/linkage/events?after=0&limit=10", "")
	var events []map[string]any
	if eventsResponse.Code != http.StatusOK || json.Unmarshal(eventsResponse.Body.Bytes(), &events) != nil || len(events) != 1 {
		t.Fatalf("GET events = %d %s", eventsResponse.Code, eventsResponse.Body.String())
	}
	if events[0]["eventId"] != event.ID || events[0]["pointKey"] != "1_1" {
		t.Fatalf("unexpected event response: %+v", events[0])
	}
	eventChannels, ok := events[0]["channels"].([]any)
	if !ok || len(eventChannels) != 1 {
		t.Fatalf("event channels response = %+v", events[0]["channels"])
	}
	channel, ok := eventChannels[0].(map[string]any)
	if !ok || channel["pointKey"] != "1_1" || channel["enabled"] != true {
		t.Fatalf("unexpected event channel response: %+v", eventChannels[0])
	}
	getEventResponse := requestHandler(t, server.Handler(), http.MethodGet, "/api/v1/linkage/events/"+event.ID, "")
	if getEventResponse.Code != http.StatusOK || !strings.Contains(getEventResponse.Body.String(), event.ID) {
		t.Fatalf("GET event = %d %s", getEventResponse.Code, getEventResponse.Body.String())
	}
}

func TestWebSocketPublishesVersionedLinkageEvent(t *testing.T) {
	server, _ := newTestServer(t)
	httpServer := httptest.NewServer(server.Handler())
	defer httpServer.Close()
	websocketURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws/linkage"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, _, err := websocket.Dial(ctx, websocketURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"http://example.com"}},
	})
	if err != nil {
		t.Fatalf("websocket.Dial() error = %v", err)
	}
	defer connection.Close(websocket.StatusNormalClosure, "test complete")
	waitForClient(t, server)

	eventTime := time.Date(2026, time.October, 19, 15, 42, 40, 382000000, time.FixedZone("CST", 8*60*60))
	published := server.Publish(store.Event{
		ID:           "evt-test-1",
		Sequence:     42,
		PointKey:     "1_1",
		StationNo:    1,
		MonitorIndex: 1,
		DeviceName:   "测试遥信点",
		EventType:    "遥信",
		Property:     "变位",
		Value:        "合位",
		EventTime:    eventTime,
		ReceivedAt:   eventTime,
		MatchStatus:  "matched",
		Channels: []store.Binding{{
			WVPDeviceID:  "wvp-device-1",
			WVPChannelID: "wvp-channel-1",
			ChannelName:  "摄像机一",
		}},
	})
	if published != 1 {
		t.Fatalf("Publish() = %d, want 1", published)
	}
	_, payload, err := connection.Read(ctx)
	if err != nil {
		t.Fatalf("WebSocket Read() error = %v", err)
	}
	var message struct {
		Type          string `json:"type"`
		SchemaVersion int    `json:"schemaVersion"`
		EventID       string `json:"eventId"`
		Sequence      int64  `json:"sequence"`
		PointKey      string `json:"pointKey"`
		Channels      []struct {
			DeviceID  string `json:"deviceId"`
			ChannelID string `json:"channelId"`
			Name      string `json:"name"`
		} `json:"channels"`
	}
	if err := json.Unmarshal(payload, &message); err != nil {
		t.Fatalf("decode WebSocket message: %v", err)
	}
	if message.Type != "linkage_event" || message.SchemaVersion != 1 || message.EventID != "evt-test-1" || message.Sequence != 42 || message.PointKey != "1_1" {
		t.Fatalf("unexpected WebSocket message: %+v", message)
	}
	if len(message.Channels) != 1 || message.Channels[0].DeviceID != "wvp-device-1" || message.Channels[0].ChannelID != "wvp-channel-1" {
		t.Fatalf("unexpected WebSocket channels: %+v", message.Channels)
	}
}

func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	ctx := context.Background()
	storage, err := store.Open(ctx, store.Options{Path: ":memory:", BusyTimeoutMS: 1000, JournalMode: "DELETE"})
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	server := NewServer(storage, config.WebConfig{
		WebSocketPath:  "/ws/linkage",
		AllowedOrigins: []string{"http://example.com"},
		ClientQueue:    4,
	}, time.FixedZone("CST", 8*60*60), nil)
	t.Cleanup(func() {
		server.Close()
		if err := storage.Close(); err != nil {
			t.Errorf("store.Close() error = %v", err)
		}
	})
	return server, storage
}

func requestHandler(t *testing.T, handler http.Handler, method string, path string, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func waitForClient(t *testing.T, server *Server) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if server.ClientCount() == 1 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("WebSocket client was not registered")
}
