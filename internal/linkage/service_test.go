package linkage

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"linkage-gateway/internal/store"
)

func TestServiceImportsPointsRecordsAndPublishesEvents(t *testing.T) {
	ctx := context.Background()
	storage, err := store.Open(ctx, store.Options{
		Path:          filepath.Join(t.TempDir(), "linkage.db"),
		BusyTimeoutMS: 1000,
		JournalMode:   "WAL",
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer storage.Close()
	location := time.FixedZone("CST", 8*60*60)
	publishedEvents := make([]store.Event, 0)
	service, err := NewService(storage, func(event store.Event) int {
		publishedEvents = append(publishedEvents, event)
		return 1
	}, location, 3*time.Second, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	pointTable := readLinkageFixture(t, "packet_0x43.bin")
	linkagePacket := readLinkageFixture(t, "packet_0x55.bin")
	receivedAt := time.Date(2026, time.October, 19, 15, 42, 43, 0, location)
	if err := service.Process(ctx, pointTable, receivedAt); err != nil {
		t.Fatalf("process point table: %v", err)
	}
	if _, err := storage.GetPoint(ctx, "1_1"); err != nil {
		t.Fatalf("imported point is unavailable: %v", err)
	}
	if err := storage.ReplaceBindings(ctx, "1_1", []store.Binding{{
		PointKey:     "1_1",
		WVPDeviceID:  "wvp-device-explicit",
		WVPChannelID: "wvp-channel-explicit",
		ChannelName:  "联动摄像机",
		Enabled:      true,
	}}, receivedAt); err != nil {
		t.Fatalf("save binding: %v", err)
	}
	if err := service.Process(ctx, linkagePacket, receivedAt); err != nil {
		t.Fatalf("process linkage packet: %v", err)
	}
	if err := service.Process(ctx, linkagePacket, receivedAt.Add(time.Second)); err != nil {
		t.Fatalf("process duplicate linkage packet: %v", err)
	}
	if len(publishedEvents) != 1 || publishedEvents[0].PointKey != "1_1" || len(publishedEvents[0].Channels) != 1 {
		t.Fatalf("published events = %+v", publishedEvents)
	}
	events, err := storage.ListEvents(ctx, 0, 10)
	if err != nil || len(events) != 2 || events[0].MatchStatus != "matched" || events[0].BroadcastStatus != "published" || events[1].MatchStatus != "duplicate" {
		t.Fatalf("stored events = %+v, error = %v", events, err)
	}
}

func readLinkageFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	return data
}
