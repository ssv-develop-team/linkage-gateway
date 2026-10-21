package web

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"linkage-gateway/internal/config"
	"linkage-gateway/internal/store"
)

type Server struct {
	storage        *store.Store
	hub            *Hub
	location       *time.Location
	allowedOrigins []string
	logger         *slog.Logger
	handler        http.Handler
}

type bindingRequest struct {
	Bindings []store.Binding `json:"bindings"`
}

type pointResponse struct {
	PointKey       string `json:"pointKey"`
	StationNo      uint16 `json:"stationNo"`
	MonitorIndex   uint16 `json:"monitorIndex"`
	DeviceName     string `json:"deviceName"`
	DeviceType     string `json:"deviceType"`
	PhysicalID     string `json:"physicalId"`
	LinkageEnabled bool   `json:"linkageEnabled"`
	ImportID       int64  `json:"importId"`
	UpdatedAt      string `json:"updatedAt"`
}

type eventResponse struct {
	EventID         string          `json:"eventId"`
	Sequence        int64           `json:"sequence"`
	PointKey        string          `json:"pointKey"`
	StationNo       uint16          `json:"stationNo"`
	MonitorIndex    uint16          `json:"monitorIndex"`
	DeviceName      string          `json:"deviceName"`
	EventType       string          `json:"eventType"`
	EventTypeCode   byte            `json:"eventTypeCode"`
	Property        string          `json:"property"`
	Value           string          `json:"value"`
	EventTime       string          `json:"eventTime"`
	ReceivedAt      string          `json:"receivedAt"`
	RawFrameHash    string          `json:"rawFrameHash"`
	MatchStatus     string          `json:"matchStatus"`
	BroadcastStatus string          `json:"broadcastStatus"`
	DuplicateOf     string          `json:"duplicateOf,omitempty"`
	Channels        []store.Binding `json:"channels"`
}

func NewServer(storage *store.Store, webConfig config.WebConfig, location *time.Location, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if location == nil {
		location = time.UTC
	}
	server := &Server{
		storage:        storage,
		location:       location,
		allowedOrigins: append([]string(nil), webConfig.AllowedOrigins...),
		logger:         logger,
	}
	server.hub = NewHub(webConfig.ClientQueue, location)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.health)
	mux.HandleFunc("GET /readyz", server.ready)
	mux.HandleFunc("GET /api/v1/linkage/points", server.listPoints)
	mux.HandleFunc("GET /api/v1/linkage/points/{point_key}", server.getPoint)
	mux.HandleFunc("GET /api/v1/linkage/bindings", server.listBindings)
	mux.HandleFunc("PUT /api/v1/linkage/bindings/{point_key}", server.replaceBindings)
	mux.HandleFunc("GET /api/v1/linkage/events", server.listEvents)
	mux.HandleFunc("GET /api/v1/linkage/events/{event_id}", server.getEvent)
	mux.HandleFunc(webConfig.WebSocketPath, server.handleWebSocket)
	server.handler = server.cors(mux)
	return server
}

func (server *Server) Handler() http.Handler {
	return server.handler
}

func (server *Server) Publish(event store.Event) int {
	return server.hub.Publish(event)
}

func (server *Server) ClientCount() int {
	return server.hub.ClientCount()
}

func (server *Server) Close() {
	server.hub.Close()
}

func (server *Server) health(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

func (server *Server) ready(writer http.ResponseWriter, request *http.Request) {
	if err := server.storage.Ping(request.Context()); err != nil {
		writeAPIError(writer, http.StatusServiceUnavailable, "database_unavailable", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ready"})
}

func (server *Server) listPoints(writer http.ResponseWriter, request *http.Request) {
	points, err := server.storage.ListPoints(request.Context())
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}
	result := make([]pointResponse, 0, len(points))
	for _, point := range points {
		result = append(result, server.pointResponse(point))
	}
	writeJSON(writer, http.StatusOK, result)
}

func (server *Server) getPoint(writer http.ResponseWriter, request *http.Request) {
	point, err := server.storage.GetPoint(request.Context(), request.PathValue("point_key"))
	if errors.Is(err, sql.ErrNoRows) {
		writeAPIError(writer, http.StatusNotFound, "point_not_found", "point was not found")
		return
	}
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, server.pointResponse(point))
}

func (server *Server) listBindings(writer http.ResponseWriter, request *http.Request) {
	pointKey := request.URL.Query().Get("point_key")
	var bindings []store.Binding
	var err error
	if pointKey == "" {
		bindings, err = server.storage.ListAllBindings(request.Context())
	} else {
		bindings, err = server.storage.ListBindings(request.Context(), pointKey)
	}
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, bindings)
}

func (server *Server) replaceBindings(writer http.ResponseWriter, request *http.Request) {
	pointKey := request.PathValue("point_key")
	var payload bindingRequest
	if err := decodeJSONBody(writer, request, 1<<20, &payload); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if len(payload.Bindings) > 128 {
		writeAPIError(writer, http.StatusBadRequest, "too_many_bindings", "at most 128 bindings are allowed")
		return
	}
	for index := range payload.Bindings {
		payload.Bindings[index].PointKey = pointKey
		if strings.TrimSpace(payload.Bindings[index].WVPDeviceID) == "" || strings.TrimSpace(payload.Bindings[index].WVPChannelID) == "" {
			writeAPIError(writer, http.StatusBadRequest, "invalid_binding", "wvpDeviceId and wvpChannelId are required")
			return
		}
	}
	if err := server.storage.ReplaceBindings(request.Context(), pointKey, payload.Bindings, time.Now()); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "binding_update_failed", err.Error())
		return
	}
	bindings, err := server.storage.ListBindings(request.Context(), pointKey)
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, bindings)
}

func (server *Server) listEvents(writer http.ResponseWriter, request *http.Request) {
	afterSequence, err := parseQueryInt(request, "after", 0)
	if err != nil || afterSequence < 0 {
		writeAPIError(writer, http.StatusBadRequest, "invalid_after", "after must be a non-negative sequence")
		return
	}
	limit, err := parseQueryInt(request, "limit", 50)
	if err != nil || limit < 1 || limit > 500 {
		writeAPIError(writer, http.StatusBadRequest, "invalid_limit", "limit must be between 1 and 500")
		return
	}
	events, err := server.storage.ListEvents(request.Context(), afterSequence, int(limit))
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}
	result := make([]eventResponse, 0, len(events))
	for _, event := range events {
		result = append(result, server.eventResponse(event))
	}
	writeJSON(writer, http.StatusOK, result)
}

func (server *Server) getEvent(writer http.ResponseWriter, request *http.Request) {
	event, err := server.storage.GetEvent(request.Context(), request.PathValue("event_id"))
	if errors.Is(err, sql.ErrNoRows) {
		writeAPIError(writer, http.StatusNotFound, "event_not_found", "event was not found")
		return
	}
	if err != nil {
		writeAPIError(writer, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, server.eventResponse(event))
}

func (server *Server) handleWebSocket(writer http.ResponseWriter, request *http.Request) {
	server.hub.handleConnection(writer, request, server.allowedOrigins)
}

func (server *Server) pointResponse(point store.Point) pointResponse {
	return pointResponse{
		PointKey:       point.PointKey(),
		StationNo:      point.StationNo,
		MonitorIndex:   point.MonitorIndex,
		DeviceName:     point.DeviceName,
		DeviceType:     point.DeviceType,
		PhysicalID:     point.PhysicalID,
		LinkageEnabled: point.Linkage,
		ImportID:       point.ImportID,
		UpdatedAt:      point.UpdatedAt.In(server.location).Format(time.RFC3339Nano),
	}
}

func (server *Server) eventResponse(event store.Event) eventResponse {
	return eventResponse{
		EventID:         event.ID,
		Sequence:        event.Sequence,
		PointKey:        event.PointKey,
		StationNo:       event.StationNo,
		MonitorIndex:    event.MonitorIndex,
		DeviceName:      event.DeviceName,
		EventType:       event.EventType,
		EventTypeCode:   event.EventTypeCode,
		Property:        event.Property,
		Value:           event.Value,
		EventTime:       event.EventTime.In(server.location).Format(time.RFC3339Nano),
		ReceivedAt:      event.ReceivedAt.In(server.location).Format(time.RFC3339Nano),
		RawFrameHash:    event.RawFrameHash,
		MatchStatus:     event.MatchStatus,
		BroadcastStatus: event.BroadcastStatus,
		DuplicateOf:     event.DuplicateOf,
		Channels:        event.Channels,
	}
}

func (server *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		origin := request.Header.Get("Origin")
		if origin != "" {
			if !server.originAllowed(origin) {
				writeAPIError(writer, http.StatusForbidden, "origin_not_allowed", "request origin is not allowed")
				return
			}
			writer.Header().Set("Access-Control-Allow-Origin", origin)
			writer.Header().Add("Vary", "Origin")
			if request.Method == http.MethodOptions {
				writer.Header().Set("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
				writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")
				writer.WriteHeader(http.StatusNoContent)
				return
			}
		}
		next.ServeHTTP(writer, request)
	})
}

func (server *Server) originAllowed(origin string) bool {
	for _, allowed := range server.allowedOrigins {
		if origin == allowed {
			return true
		}
		matched, err := path.Match(allowed, origin)
		if err == nil && matched {
			return true
		}
	}
	return false
}

func decodeJSONBody(writer http.ResponseWriter, request *http.Request, maxBytes int64, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("request body must contain a single JSON value")
		}
		return err
	}
	return nil
}

func parseQueryInt(request *http.Request, name string, defaultValue int64) (int64, error) {
	value := request.URL.Query().Get(name)
	if value == "" {
		return defaultValue, nil
	}
	return strconv.ParseInt(value, 10, 64)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return
	}
}

func writeAPIError(writer http.ResponseWriter, status int, code string, message string) {
	writeJSON(writer, status, map[string]string{"code": code, "error": message})
}

func (server *Server) Shutdown(ctx context.Context) error {
	server.hub.Close()
	return nil
}
