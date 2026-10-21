package protocol

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	return data
}

func TestParsePointTableFixture(t *testing.T) {
	packet := readFixture(t, "packet_0x43.bin")
	frame, err := ParsePointTableFrame(packet)
	if err != nil {
		t.Fatalf("ParsePointTableFrame() error = %v", err)
	}
	if frame.StationNo != 1 || frame.HasMore || frame.Sequence != 0 || frame.Data == nil || len(frame.Data) != 221 {
		t.Fatalf("unexpected frame metadata: %+v; data length=%d", frame, len(frame.Data))
	}
	points, err := ParseDevicePoints(frame.Data)
	if err != nil {
		t.Fatalf("ParseDevicePoints() error = %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("point count = %d, want 1", len(points))
	}
	point := points[0]
	if point.PointKey() != "1_1" || point.DeviceName != "zh/110KV..12140.地刀" || point.DeviceType != "遥控" || point.PhysicalID != "1" || !point.Linkage {
		t.Fatalf("unexpected point: %+v", point)
	}
}

func TestParseLinkageFixture(t *testing.T) {
	packet := readFixture(t, "packet_0x55.bin")
	frame, err := ParseLinkageFrame(packet)
	if err != nil {
		t.Fatalf("ParseLinkageFrame() error = %v", err)
	}
	if frame.PointKey() != "1_1" || frame.StationNo != 1 || frame.MonitorIndex != 1 || frame.EventType != EventSignal || frame.Property != "变位" || frame.Value != "合位" {
		t.Fatalf("unexpected linkage event: %+v", frame)
	}
	if frame.Timestamp.Milliseconds != 40382 || frame.Timestamp.Minute != 42 || frame.Timestamp.Hour != 15 || frame.Timestamp.Day != 19 || frame.Timestamp.Month != 10 || frame.Timestamp.Year != 26 || frame.Timestamp.Invalid || frame.Timestamp.SummerTime {
		t.Fatalf("unexpected CP56Time2a fields: %+v", frame.Timestamp)
	}
	location := time.FixedZone("CST", 8*60*60)
	got, err := frame.Timestamp.Time(location)
	if err != nil {
		t.Fatalf("Timestamp.Time() error = %v", err)
	}
	want := time.Date(2026, time.October, 19, 15, 42, 40, 382000000, location)
	if !got.Equal(want) {
		t.Fatalf("timestamp = %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func TestParseFrameRejectsCorruptChecksum(t *testing.T) {
	packet := readFixture(t, "packet_0x55.bin")
	packet[11] ^= 0x01
	_, err := ParseLinkageFrame(packet)
	if !errors.Is(err, ErrInvalidChecksum) {
		t.Fatalf("ParseLinkageFrame() error = %v, want checksum error", err)
	}
}

func TestParsePointTableFrameRejectsMismatchedLength(t *testing.T) {
	packet := readFixture(t, "packet_0x43.bin")
	packet[14]--
	var checksum byte
	for _, value := range packet[5 : len(packet)-1] {
		checksum += value
	}
	packet[len(packet)-1] = checksum
	_, err := ParsePointTableFrame(packet)
	if !errors.Is(err, ErrInvalidFrame) {
		t.Fatalf("ParsePointTableFrame() error = %v, want frame-length error", err)
	}
}

func TestCP56Time2aRejectsInvalidAndMissingLocation(t *testing.T) {
	invalid := CP56Time2a{Invalid: true}
	if _, err := invalid.Time(time.UTC); err == nil {
		t.Fatal("Time() accepted an invalid CP56Time2a timestamp")
	}
	valid := CP56Time2a{Milliseconds: 1, Minute: 1, Hour: 1, Day: 1, Month: 1}
	if _, err := valid.Time(nil); err == nil {
		t.Fatal("Time() accepted a nil location")
	}
}
