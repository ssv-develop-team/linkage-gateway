package linkage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"linkage-gateway/internal/protocol"
	"linkage-gateway/internal/store"
)

type Service struct {
	storage     *store.Store
	publish     func(store.Event) int
	location    *time.Location
	dedupWindow time.Duration
	logger      *slog.Logger
}

func NewService(storage *store.Store, publish func(store.Event) int, location *time.Location, dedupWindow time.Duration, logger *slog.Logger) (*Service, error) {
	if storage == nil {
		return nil, errors.New("linkage service requires a store")
	}
	if location == nil {
		return nil, errors.New("linkage service requires an event timezone")
	}
	if dedupWindow < 0 {
		return nil, errors.New("deduplication window must not be negative")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{storage: storage, publish: publish, location: location, dedupWindow: dedupWindow, logger: logger}, nil
}

func (service *Service) Process(ctx context.Context, packet []byte, receivedAt time.Time) error {
	if len(packet) < 5 {
		return fmt.Errorf("UDP datagram is too short to contain a protocol type")
	}
	switch packet[4] {
	case protocol.TypePointTable:
		return service.processPointTable(ctx, packet, receivedAt)
	case protocol.TypeLinkage:
		return service.processLinkage(ctx, packet, receivedAt)
	default:
		return fmt.Errorf("unsupported protocol type 0x%02x", packet[4])
	}
}

func (service *Service) processPointTable(ctx context.Context, packet []byte, receivedAt time.Time) error {
	frame, err := protocol.ParsePointTableFrame(packet)
	if err != nil {
		return err
	}
	if frame.HasMore || frame.Sequence != 0 || frame.StartBytes != [4]byte{} {
		return errors.New("multi-frame point-table imports are not supported yet")
	}
	points, err := protocol.ParseDevicePoints(frame.Data)
	if err != nil {
		return err
	}
	for _, point := range points {
		if point.StationNo != frame.StationNo {
			return fmt.Errorf("point %s station number does not match frame station %d", point.PointKey(), frame.StationNo)
		}
	}
	frameHash := sha256.Sum256(packet)
	importID, err := service.storage.ImportPoints(ctx, frame.StationNo, points, hex.EncodeToString(frameHash[:]), receivedAt)
	if err != nil {
		return err
	}
	service.logger.Info("point table imported", "station_no", frame.StationNo, "point_count", len(points), "import_id", importID)
	return nil
}

func (service *Service) processLinkage(ctx context.Context, packet []byte, receivedAt time.Time) error {
	frame, err := protocol.ParseLinkageFrame(packet)
	if err != nil {
		return err
	}
	eventTime, err := frame.Timestamp.Time(service.location)
	if err != nil {
		return fmt.Errorf("convert event timestamp: %w", err)
	}
	pointKey := frame.PointKey()
	point, err := service.storage.GetPoint(ctx, pointKey)
	matchStatus := "matched"
	channels := make([]store.Binding, 0)
	deviceName := ""
	if errors.Is(err, sql.ErrNoRows) {
		matchStatus = "unmatched"
	} else if err != nil {
		return fmt.Errorf("find point %s: %w", pointKey, err)
	} else {
		deviceName = point.DeviceName
		if !point.Linkage {
			matchStatus = "disabled"
		} else {
			bindings, err := service.storage.ListBindings(ctx, pointKey)
			if err != nil {
				return fmt.Errorf("list point bindings: %w", err)
			}
			for _, binding := range bindings {
				if binding.Enabled {
					channels = append(channels, binding)
				}
			}
			if len(channels) == 0 {
				matchStatus = "unbound"
			}
		}
	}
	frameHash := sha256.Sum256(packet)
	event, duplicate, err := service.storage.InsertEvent(ctx, store.Event{
		PointKey:        pointKey,
		StationNo:       frame.StationNo,
		MonitorIndex:    frame.MonitorIndex,
		DeviceName:      deviceName,
		EventType:       frame.EventType.String(),
		EventTypeCode:   byte(frame.EventType),
		Property:        frame.Property,
		Value:           frame.Value,
		EventTime:       eventTime,
		ReceivedAt:      receivedAt,
		RawFrameHash:    hex.EncodeToString(frameHash[:]),
		MatchStatus:     matchStatus,
		BroadcastStatus: "pending",
		Channels:        channels,
	}, service.dedupWindow)
	if err != nil {
		return err
	}
	if duplicate {
		service.logger.Info("duplicate linkage event recorded", "event_id", event.ID, "duplicate_of", event.DuplicateOf, "point_key", pointKey)
		return nil
	}
	publishedCount := 0
	if service.publish != nil {
		publishedCount = service.publish(event)
	}
	broadcastStatus := "no_clients"
	if publishedCount > 0 {
		broadcastStatus = "published"
	}
	if err := service.storage.UpdateBroadcastStatus(ctx, event.Sequence, broadcastStatus); err != nil {
		return err
	}
	service.logger.Info("linkage event processed", "event_id", event.ID, "point_key", pointKey, "match_status", matchStatus, "channels", len(channels), "websocket_clients", publishedCount)
	return nil
}

func ServeUDP(ctx context.Context, address string, maxPacketBytes int, service *Service, logger *slog.Logger) error {
	if service == nil {
		return errors.New("UDP server requires a linkage service")
	}
	if logger == nil {
		logger = slog.Default()
	}
	udpAddress, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		return fmt.Errorf("resolve UDP address %q: %w", address, err)
	}
	connection, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		return fmt.Errorf("listen on UDP %s: %w", address, err)
	}
	defer connection.Close()
	shutdownComplete := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			connection.Close()
		case <-shutdownComplete:
		}
	}()
	defer close(shutdownComplete)

	buffer := make([]byte, maxPacketBytes)
	for {
		packetLength, remoteAddress, err := connection.ReadFromUDP(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read UDP datagram: %w", err)
		}
		packet := append([]byte(nil), buffer[:packetLength]...)
		if err := service.Process(ctx, packet, time.Now()); err != nil {
			logger.Warn("UDP datagram rejected", "remote", remoteAddress.String(), "bytes", packetLength, "error", err)
		}
	}
}
