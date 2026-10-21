package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"linkage-gateway/internal/protocol"
)

func TestStorePersistsPointBindingsAndEvents(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "linkage.db")
	options := Options{Path: databasePath, BusyTimeoutMS: 1000, JournalMode: "WAL"}
	storage, err := Open(ctx, options)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	point := protocol.DevicePoint{
		StationNo:    1,
		MonitorIndex: 1,
		DeviceName:   "测试设备",
		DeviceType:   "遥信",
		PhysicalID:   "physical-1",
		Linkage:      true,
	}
	receivedAt := time.Date(2026, time.October, 19, 15, 42, 40, 382000000, time.FixedZone("CST", 8*60*60))
	importID, err := storage.ImportPoints(ctx, 1, []protocol.DevicePoint{point}, "point-hash", receivedAt)
	if err != nil {
		t.Fatalf("ImportPoints() error = %v", err)
	}
	storedPoint, err := storage.GetPoint(ctx, "1_1")
	if err != nil {
		t.Fatalf("GetPoint() error = %v", err)
	}
	if storedPoint.ImportID != importID || storedPoint.PointKey() != "1_1" || storedPoint.DeviceName != point.DeviceName {
		t.Fatalf("unexpected stored point: %+v", storedPoint)
	}

	bindings := []Binding{{
		PointKey:     "1_1",
		WVPDeviceID:  "device-explicit",
		WVPChannelID: "channel-explicit",
		ChannelName:  "联动摄像机",
		Enabled:      true,
		DisplayOrder: 1,
	}}
	if err := storage.ReplaceBindings(ctx, "1_1", bindings, receivedAt); err != nil {
		t.Fatalf("ReplaceBindings() error = %v", err)
	}
	storedBindings, err := storage.ListBindings(ctx, "1_1")
	if err != nil || len(storedBindings) != 1 || storedBindings[0].WVPChannelID != "channel-explicit" {
		t.Fatalf("ListBindings() = %+v, error = %v", storedBindings, err)
	}

	event := Event{
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
		BroadcastStatus: "pending",
		Channels:        storedBindings,
	}
	storedEvent, duplicate, err := storage.InsertEvent(ctx, event, 3*time.Second)
	if err != nil || duplicate || storedEvent.ID == "" || storedEvent.Sequence != 1 {
		t.Fatalf("InsertEvent() = %+v, duplicate=%t, error=%v", storedEvent, duplicate, err)
	}
	if err := storage.UpdateBroadcastStatus(ctx, storedEvent.Sequence, "published"); err != nil {
		t.Fatalf("UpdateBroadcastStatus() error = %v", err)
	}
	duplicateEvent, duplicate, err := storage.InsertEvent(ctx, event, 3*time.Second)
	if err != nil || !duplicate || duplicateEvent.DuplicateOf != storedEvent.ID || duplicateEvent.MatchStatus != "duplicate" {
		t.Fatalf("duplicate InsertEvent() = %+v, duplicate=%t, error=%v", duplicateEvent, duplicate, err)
	}
	if err := storage.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	storage, err = Open(ctx, options)
	if err != nil {
		t.Fatalf("reopen database: %v", err)
	}
	defer storage.Close()
	storedPoint, err = storage.GetPoint(ctx, "1_1")
	if err != nil || storedPoint.DeviceName != point.DeviceName {
		t.Fatalf("point after restart = %+v, error = %v", storedPoint, err)
	}
	storedEvent, err = storage.GetEvent(ctx, storedEvent.ID)
	if err != nil || storedEvent.BroadcastStatus != "published" || len(storedEvent.Channels) != 1 ||
		storedEvent.Channels[0].PointKey != "1_1" || !storedEvent.Channels[0].Enabled {
		t.Fatalf("event after restart = %+v, error = %v", storedEvent, err)
	}
	events, err := storage.ListEvents(ctx, 0, 50)
	if err != nil || len(events) != 2 || events[1].DuplicateOf != storedEvent.ID {
		t.Fatalf("ListEvents() = %+v, error = %v", events, err)
	}
}
