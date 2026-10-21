package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"linkage-gateway/internal/protocol"

	_ "modernc.org/sqlite"
)

type Options struct {
	Path          string
	BusyTimeoutMS int
	JournalMode   string
}

type Store struct {
	db *sql.DB
}

type Point struct {
	protocol.DevicePoint
	ImportID  int64     `json:"importId"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type Binding struct {
	PointKey     string `json:"pointKey"`
	WVPDeviceID  string `json:"deviceId"`
	WVPChannelID string `json:"channelId"`
	ChannelName  string `json:"name"`
	Enabled      bool   `json:"enabled"`
	DisplayOrder int    `json:"displayOrder"`
}

type Event struct {
	ID              string    `json:"eventId"`
	Sequence        int64     `json:"sequence"`
	PointKey        string    `json:"pointKey"`
	StationNo       uint16    `json:"stationNo"`
	MonitorIndex    uint16    `json:"monitorIndex"`
	DeviceName      string    `json:"deviceName"`
	EventType       string    `json:"eventType"`
	EventTypeCode   byte      `json:"eventTypeCode"`
	Property        string    `json:"property"`
	Value           string    `json:"value"`
	EventTime       time.Time `json:"eventTime"`
	ReceivedAt      time.Time `json:"receivedAt"`
	RawFrameHash    string    `json:"rawFrameHash"`
	MatchStatus     string    `json:"matchStatus"`
	BroadcastStatus string    `json:"broadcastStatus"`
	DuplicateOf     string    `json:"duplicateOf,omitempty"`
	Channels        []Binding `json:"channels"`
}

func Open(ctx context.Context, options Options) (*Store, error) {
	if strings.TrimSpace(options.Path) == "" {
		return nil, errors.New("database path must not be empty")
	}
	if options.Path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(options.Path), 0750); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	database, err := sql.Open("sqlite", options.Path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite database: %w", err)
	}
	database.SetMaxOpenConns(1)
	store := &Store{db: database}
	closeOnError := func(err error) (*Store, error) {
		database.Close()
		return nil, err
	}
	if err := database.PingContext(ctx); err != nil {
		return closeOnError(fmt.Errorf("connect to SQLite database: %w", err))
	}
	if _, err := database.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", options.BusyTimeoutMS)); err != nil {
		return closeOnError(fmt.Errorf("set SQLite busy timeout: %w", err))
	}
	if _, err := database.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return closeOnError(fmt.Errorf("enable SQLite foreign keys: %w", err))
	}
	journalMode := strings.ToUpper(options.JournalMode)
	if journalMode != "WAL" && journalMode != "DELETE" {
		return closeOnError(errors.New("SQLite journal mode must be WAL or DELETE"))
	}
	if options.Path == ":memory:" {
		journalMode = "DELETE"
	}
	if _, err := database.ExecContext(ctx, "PRAGMA journal_mode = "+journalMode); err != nil {
		return closeOnError(fmt.Errorf("set SQLite journal mode: %w", err))
	}
	if err := store.migrate(ctx); err != nil {
		return closeOnError(err)
	}
	return store, nil
}

func (store *Store) Close() error {
	return store.db.Close()
}

func (store *Store) Ping(ctx context.Context) error {
	return store.db.PingContext(ctx)
}

func (store *Store) migrate(ctx context.Context) error {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin database migration: %w", err)
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}
	var currentVersion int
	if err := transaction.QueryRowContext(ctx, "SELECT COALESCE(MAX(version), 0) FROM schema_migrations").Scan(&currentVersion); err != nil {
		return fmt.Errorf("read migration version: %w", err)
	}
	if currentVersion > 1 {
		return fmt.Errorf("database schema version %d is newer than this gateway supports", currentVersion)
	}
	if currentVersion == 0 {
		if _, err := transaction.ExecContext(ctx, schemaV1); err != nil {
			return fmt.Errorf("apply database migration 1: %w", err)
		}
		if _, err := transaction.ExecContext(ctx, "INSERT INTO schema_migrations(version, applied_at) VALUES(1, ?)", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return fmt.Errorf("record database migration 1: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit database migration: %w", err)
	}
	return nil
}

const schemaV1 = `
CREATE TABLE point_imports (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	station_no INTEGER NOT NULL,
	received_at TEXT NOT NULL,
	frame_count INTEGER NOT NULL,
	raw_frame_hash TEXT NOT NULL,
	point_count INTEGER NOT NULL
);
CREATE TABLE point_table (
	point_key TEXT PRIMARY KEY,
	station_no INTEGER NOT NULL,
	monitor_index INTEGER NOT NULL,
	device_name TEXT NOT NULL,
	device_type TEXT NOT NULL,
	physical_id TEXT NOT NULL,
	linkage_enabled INTEGER NOT NULL,
	import_id INTEGER NOT NULL REFERENCES point_imports(id),
	updated_at TEXT NOT NULL
);
CREATE INDEX idx_point_table_station_index ON point_table(station_no, monitor_index);
CREATE TABLE point_bindings (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	point_key TEXT NOT NULL,
	wvp_device_id TEXT NOT NULL,
	wvp_channel_id TEXT NOT NULL,
	channel_name TEXT NOT NULL,
	enabled INTEGER NOT NULL,
	display_order INTEGER NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	UNIQUE(point_key, wvp_device_id, wvp_channel_id)
);
CREATE INDEX idx_point_bindings_point_enabled ON point_bindings(point_key, enabled, display_order);
CREATE TABLE linkage_events (
	sequence INTEGER PRIMARY KEY AUTOINCREMENT,
	event_id TEXT UNIQUE,
	point_key TEXT NOT NULL,
	station_no INTEGER NOT NULL,
	monitor_index INTEGER NOT NULL,
	device_name TEXT NOT NULL,
	event_type TEXT NOT NULL,
	event_type_code INTEGER NOT NULL,
	property TEXT NOT NULL,
	value TEXT NOT NULL,
	event_time TEXT NOT NULL,
	received_at TEXT NOT NULL,
	raw_frame_hash TEXT NOT NULL,
	match_status TEXT NOT NULL,
	broadcast_status TEXT NOT NULL,
	duplicate_of TEXT
);
CREATE INDEX idx_linkage_events_point_sequence ON linkage_events(point_key, sequence);
CREATE INDEX idx_linkage_events_received_at ON linkage_events(received_at);
CREATE INDEX idx_linkage_events_duplicate_check ON linkage_events(raw_frame_hash, received_at, duplicate_of);
CREATE TABLE linkage_event_channels (
	sequence INTEGER NOT NULL REFERENCES linkage_events(sequence) ON DELETE CASCADE,
	wvp_device_id TEXT NOT NULL,
	wvp_channel_id TEXT NOT NULL,
	channel_name TEXT NOT NULL,
	display_order INTEGER NOT NULL,
	PRIMARY KEY(sequence, wvp_device_id, wvp_channel_id)
);
`

func (store *Store) ImportPoints(ctx context.Context, stationNo uint16, points []protocol.DevicePoint, rawFrameHash string, receivedAt time.Time) (int64, error) {
	if len(points) == 0 {
		return 0, errors.New("point import must contain at least one point")
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin point import: %w", err)
	}
	defer transaction.Rollback()
	result, err := transaction.ExecContext(ctx,
		"INSERT INTO point_imports(station_no, received_at, frame_count, raw_frame_hash, point_count) VALUES(?, ?, 1, ?, ?)",
		stationNo, timeText(receivedAt), rawFrameHash, len(points))
	if err != nil {
		return 0, fmt.Errorf("insert point import: %w", err)
	}
	importID, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("get point import ID: %w", err)
	}
	statement, err := transaction.PrepareContext(ctx, `INSERT INTO point_table(
		point_key, station_no, monitor_index, device_name, device_type, physical_id, linkage_enabled, import_id, updated_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(point_key) DO UPDATE SET
		station_no = excluded.station_no,
		monitor_index = excluded.monitor_index,
		device_name = excluded.device_name,
		device_type = excluded.device_type,
		physical_id = excluded.physical_id,
		linkage_enabled = excluded.linkage_enabled,
		import_id = excluded.import_id,
		updated_at = excluded.updated_at`)
	if err != nil {
		return 0, fmt.Errorf("prepare point upsert: %w", err)
	}
	defer statement.Close()
	for _, point := range points {
		if point.StationNo != stationNo {
			return 0, fmt.Errorf("point %s station number does not match frame station %d", point.PointKey(), stationNo)
		}
		linkageEnabled := 0
		if point.Linkage {
			linkageEnabled = 1
		}
		if _, err := statement.ExecContext(ctx, point.PointKey(), point.StationNo, point.MonitorIndex, point.DeviceName, point.DeviceType, point.PhysicalID, linkageEnabled, importID, timeText(receivedAt)); err != nil {
			return 0, fmt.Errorf("upsert point %s: %w", point.PointKey(), err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return 0, fmt.Errorf("commit point import: %w", err)
	}
	return importID, nil
}

func (store *Store) ListPoints(ctx context.Context) ([]Point, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT station_no, monitor_index, device_name, device_type, physical_id, linkage_enabled, import_id, updated_at
	FROM point_table ORDER BY station_no, monitor_index`)
	if err != nil {
		return nil, fmt.Errorf("query points: %w", err)
	}
	defer rows.Close()
	points := make([]Point, 0)
	for rows.Next() {
		point, err := scanPoint(rows)
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read points: %w", err)
	}
	return points, nil
}

func (store *Store) GetPoint(ctx context.Context, pointKey string) (Point, error) {
	row := store.db.QueryRowContext(ctx, `SELECT station_no, monitor_index, device_name, device_type, physical_id, linkage_enabled, import_id, updated_at
	FROM point_table WHERE point_key = ?`, pointKey)
	return scanPoint(row)
}

func scanPoint(scanner interface{ Scan(...any) error }) (Point, error) {
	var point Point
	var stationNo int64
	var monitorIndex int64
	var linkageEnabled int
	var updatedAt string
	if err := scanner.Scan(&stationNo, &monitorIndex, &point.DeviceName, &point.DeviceType, &point.PhysicalID, &linkageEnabled, &point.ImportID, &updatedAt); err != nil {
		return Point{}, err
	}
	parsedTime, err := time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return Point{}, fmt.Errorf("parse point update time: %w", err)
	}
	point.StationNo = uint16(stationNo)
	point.MonitorIndex = uint16(monitorIndex)
	point.Linkage = linkageEnabled != 0
	point.UpdatedAt = parsedTime
	return point, nil
}

func (store *Store) ReplaceBindings(ctx context.Context, pointKey string, bindings []Binding, updatedAt time.Time) error {
	seen := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		if binding.PointKey != "" && binding.PointKey != pointKey {
			return errors.New("binding point key does not match request point key")
		}
		if strings.TrimSpace(binding.WVPDeviceID) == "" || strings.TrimSpace(binding.WVPChannelID) == "" {
			return errors.New("WVP device ID and channel ID are required")
		}
		key := binding.WVPDeviceID + "\x00" + binding.WVPChannelID
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate WVP channel binding for point %s", pointKey)
		}
		seen[key] = struct{}{}
	}
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin binding update: %w", err)
	}
	defer transaction.Rollback()
	if _, err := transaction.ExecContext(ctx, "DELETE FROM point_bindings WHERE point_key = ?", pointKey); err != nil {
		return fmt.Errorf("delete existing bindings: %w", err)
	}
	statement, err := transaction.PrepareContext(ctx, `INSERT INTO point_bindings(
		point_key, wvp_device_id, wvp_channel_id, channel_name, enabled, display_order, created_at, updated_at
	) VALUES(?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("prepare binding insert: %w", err)
	}
	defer statement.Close()
	timestamp := timeText(updatedAt)
	for _, binding := range bindings {
		enabled := 0
		if binding.Enabled {
			enabled = 1
		}
		if _, err := statement.ExecContext(ctx, pointKey, binding.WVPDeviceID, binding.WVPChannelID, binding.ChannelName, enabled, binding.DisplayOrder, timestamp, timestamp); err != nil {
			return fmt.Errorf("insert WVP channel binding: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit binding update: %w", err)
	}
	return nil
}

func (store *Store) ListBindings(ctx context.Context, pointKey string) ([]Binding, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT point_key, wvp_device_id, wvp_channel_id, channel_name, enabled, display_order
	FROM point_bindings WHERE point_key = ? ORDER BY display_order, id`, pointKey)
	if err != nil {
		return nil, fmt.Errorf("query bindings: %w", err)
	}
	defer rows.Close()
	bindings := make([]Binding, 0)
	for rows.Next() {
		var binding Binding
		var enabled int
		if err := rows.Scan(&binding.PointKey, &binding.WVPDeviceID, &binding.WVPChannelID, &binding.ChannelName, &enabled, &binding.DisplayOrder); err != nil {
			return nil, fmt.Errorf("read binding: %w", err)
		}
		binding.Enabled = enabled != 0
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read bindings: %w", err)
	}
	return bindings, nil
}

func (store *Store) ListAllBindings(ctx context.Context) ([]Binding, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT point_key, wvp_device_id, wvp_channel_id, channel_name, enabled, display_order
	FROM point_bindings ORDER BY point_key, display_order, id`)
	if err != nil {
		return nil, fmt.Errorf("query bindings: %w", err)
	}
	defer rows.Close()
	bindings := make([]Binding, 0)
	for rows.Next() {
		var binding Binding
		var enabled int
		if err := rows.Scan(&binding.PointKey, &binding.WVPDeviceID, &binding.WVPChannelID, &binding.ChannelName, &enabled, &binding.DisplayOrder); err != nil {
			return nil, fmt.Errorf("read binding: %w", err)
		}
		binding.Enabled = enabled != 0
		bindings = append(bindings, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read bindings: %w", err)
	}
	return bindings, nil
}

func (store *Store) InsertEvent(ctx context.Context, event Event, dedupWindow time.Duration) (Event, bool, error) {
	transaction, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return Event{}, false, fmt.Errorf("begin event insert: %w", err)
	}
	defer transaction.Rollback()
	if dedupWindow > 0 {
		cutoff := timeText(event.ReceivedAt.Add(-dedupWindow))
		if err := transaction.QueryRowContext(ctx, `SELECT event_id FROM linkage_events
			WHERE raw_frame_hash = ? AND received_at >= ? AND duplicate_of IS NULL
			ORDER BY sequence DESC LIMIT 1`, event.RawFrameHash, cutoff).Scan(&event.DuplicateOf); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return Event{}, false, fmt.Errorf("check duplicate event: %w", err)
		}
	}
	if event.DuplicateOf != "" {
		event.MatchStatus = "duplicate"
		event.BroadcastStatus = "duplicate"
	} else {
		event.BroadcastStatus = "pending"
	}
	result, err := transaction.ExecContext(ctx, `INSERT INTO linkage_events(
		event_id, point_key, station_no, monitor_index, device_name, event_type, event_type_code,
		property, value, event_time, received_at, raw_frame_hash, match_status, broadcast_status, duplicate_of
	) VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		event.PointKey, event.StationNo, event.MonitorIndex, event.DeviceName, event.EventType, event.EventTypeCode,
		event.Property, event.Value, timeText(event.EventTime), timeText(event.ReceivedAt), event.RawFrameHash,
		event.MatchStatus, event.BroadcastStatus, nullableString(event.DuplicateOf))
	if err != nil {
		return Event{}, false, fmt.Errorf("insert linkage event: %w", err)
	}
	event.Sequence, err = result.LastInsertId()
	if err != nil {
		return Event{}, false, fmt.Errorf("get linkage event sequence: %w", err)
	}
	event.ID = fmt.Sprintf("evt-%012d", event.Sequence)
	if _, err := transaction.ExecContext(ctx, "UPDATE linkage_events SET event_id = ? WHERE sequence = ?", event.ID, event.Sequence); err != nil {
		return Event{}, false, fmt.Errorf("assign linkage event ID: %w", err)
	}
	if event.DuplicateOf == "" {
		statement, err := transaction.PrepareContext(ctx, `INSERT INTO linkage_event_channels(
			sequence, wvp_device_id, wvp_channel_id, channel_name, display_order
		) VALUES(?, ?, ?, ?, ?)`)
		if err != nil {
			return Event{}, false, fmt.Errorf("prepare event channels: %w", err)
		}
		for _, binding := range event.Channels {
			if !binding.Enabled {
				continue
			}
			if _, err := statement.ExecContext(ctx, event.Sequence, binding.WVPDeviceID, binding.WVPChannelID, binding.ChannelName, binding.DisplayOrder); err != nil {
				statement.Close()
				return Event{}, false, fmt.Errorf("insert event channel: %w", err)
			}
		}
		if err := statement.Close(); err != nil {
			return Event{}, false, fmt.Errorf("close event channels statement: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return Event{}, false, fmt.Errorf("commit linkage event: %w", err)
	}
	return event, event.DuplicateOf != "", nil
}

func (store *Store) UpdateBroadcastStatus(ctx context.Context, sequence int64, status string) error {
	if _, err := store.db.ExecContext(ctx, "UPDATE linkage_events SET broadcast_status = ? WHERE sequence = ?", status, sequence); err != nil {
		return fmt.Errorf("update event broadcast status: %w", err)
	}
	return nil
}

func (store *Store) ListEvents(ctx context.Context, afterSequence int64, limit int) ([]Event, error) {
	if limit < 1 || limit > 500 {
		return nil, errors.New("event limit must be between 1 and 500")
	}
	rows, err := store.db.QueryContext(ctx, `SELECT sequence, event_id, point_key, station_no, monitor_index, device_name,
		event_type, event_type_code, property, value, event_time, received_at, raw_frame_hash,
		match_status, broadcast_status, duplicate_of
	FROM linkage_events WHERE sequence > ? ORDER BY sequence LIMIT ?`, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("query linkage events: %w", err)
	}
	defer rows.Close()
	events := make([]Event, 0)
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read linkage events: %w", err)
	}
	for index := range events {
		channels, err := store.eventChannels(ctx, events[index].Sequence)
		if err != nil {
			return nil, err
		}
		events[index].Channels = channels
	}
	return events, nil
}

func (store *Store) GetEvent(ctx context.Context, eventID string) (Event, error) {
	row := store.db.QueryRowContext(ctx, `SELECT sequence, event_id, point_key, station_no, monitor_index, device_name,
		event_type, event_type_code, property, value, event_time, received_at, raw_frame_hash,
		match_status, broadcast_status, duplicate_of
	FROM linkage_events WHERE event_id = ?`, eventID)
	event, err := scanEvent(row)
	if err != nil {
		return Event{}, err
	}
	event.Channels, err = store.eventChannels(ctx, event.Sequence)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

func scanEvent(scanner interface{ Scan(...any) error }) (Event, error) {
	var event Event
	var stationNo int64
	var monitorIndex int64
	var eventTypeCode int
	var eventTime string
	var receivedAt string
	var duplicateOf sql.NullString
	if err := scanner.Scan(&event.Sequence, &event.ID, &event.PointKey, &stationNo, &monitorIndex, &event.DeviceName,
		&event.EventType, &eventTypeCode, &event.Property, &event.Value, &eventTime, &receivedAt, &event.RawFrameHash,
		&event.MatchStatus, &event.BroadcastStatus, &duplicateOf); err != nil {
		return Event{}, err
	}
	var err error
	event.EventTime, err = time.Parse(time.RFC3339Nano, eventTime)
	if err != nil {
		return Event{}, fmt.Errorf("parse event time: %w", err)
	}
	event.ReceivedAt, err = time.Parse(time.RFC3339Nano, receivedAt)
	if err != nil {
		return Event{}, fmt.Errorf("parse receive time: %w", err)
	}
	event.StationNo = uint16(stationNo)
	event.MonitorIndex = uint16(monitorIndex)
	event.EventTypeCode = byte(eventTypeCode)
	event.DuplicateOf = duplicateOf.String
	return event, nil
}

func (store *Store) eventChannels(ctx context.Context, sequence int64) ([]Binding, error) {
	rows, err := store.db.QueryContext(ctx, `SELECT linkage_events.point_key, linkage_event_channels.wvp_device_id,
	linkage_event_channels.wvp_channel_id, linkage_event_channels.channel_name, linkage_event_channels.display_order
	FROM linkage_event_channels JOIN linkage_events USING(sequence)
	WHERE sequence = ? ORDER BY display_order`, sequence)
	if err != nil {
		return nil, fmt.Errorf("query event channels: %w", err)
	}
	defer rows.Close()
	channels := make([]Binding, 0)
	for rows.Next() {
		var binding Binding
		if err := rows.Scan(&binding.PointKey, &binding.WVPDeviceID, &binding.WVPChannelID, &binding.ChannelName, &binding.DisplayOrder); err != nil {
			return nil, fmt.Errorf("read event channel: %w", err)
		}
		binding.Enabled = true
		channels = append(channels, binding)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read event channels: %w", err)
	}
	return channels, nil
}

func timeText(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
