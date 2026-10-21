package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/encoding/simplifiedchinese"
)

const (
	TypePointTable    byte = 0x43
	TypeLinkage       byte = 0x55
	frameHeaderLen         = 5
	checksumLen            = 1
	pointTableDataMax      = 240
)

var framePreamble = [4]byte{0xeb, 0x90, 0xeb, 0x90}

var (
	ErrInvalidFrame      = errors.New("invalid linkage protocol frame")
	ErrInvalidChecksum   = errors.New("invalid linkage protocol checksum")
	ErrInvalidText       = errors.New("invalid GB2312 text")
	ErrInvalidPointTable = errors.New("invalid device resource information")
)

type DevicePoint struct {
	StationNo    uint16
	MonitorIndex uint16
	DeviceName   string
	DeviceType   string
	PhysicalID   string
	Linkage      bool
}

func (point DevicePoint) PointKey() string {
	return fmt.Sprintf("%d_%d", point.StationNo, point.MonitorIndex)
}

type PointTableFrame struct {
	StationNo  uint16
	HasMore    bool
	Sequence   byte
	StartBytes [4]byte
	Data       []byte
}

type EventType byte

const (
	EventRemoteControl EventType = 0
	EventSignal        EventType = 1
	EventMeasurement   EventType = 2
)

func (eventType EventType) String() string {
	switch eventType {
	case EventRemoteControl:
		return "遥控"
	case EventSignal:
		return "遥信"
	case EventMeasurement:
		return "遥测"
	default:
		return "未知"
	}
}

type CP56Time2a struct {
	Milliseconds uint16
	Minute       byte
	Hour         byte
	Day          byte
	Weekday      byte
	Month        byte
	Year         byte
	Invalid      bool
	SummerTime   bool
}

func ParseCP56Time2a(raw []byte) (CP56Time2a, error) {
	if len(raw) != 7 {
		return CP56Time2a{}, fmt.Errorf("CP56Time2a must be 7 bytes, got %d: %w", len(raw), ErrInvalidFrame)
	}

	timestamp := CP56Time2a{
		Milliseconds: binary.LittleEndian.Uint16(raw[0:2]),
		Minute:       raw[2] & 0x3f,
		Hour:         raw[3] & 0x1f,
		Day:          raw[4] & 0x1f,
		Weekday:      raw[4] >> 5,
		Month:        raw[5] & 0x0f,
		Year:         raw[6] & 0x7f,
		Invalid:      raw[2]&0x80 != 0,
		SummerTime:   raw[3]&0x80 != 0,
	}
	if timestamp.Milliseconds > 59999 || timestamp.Minute > 59 || timestamp.Hour > 23 || timestamp.Day < 1 || timestamp.Day > 31 || timestamp.Weekday > 7 || timestamp.Month < 1 || timestamp.Month > 12 || timestamp.Year > 99 {
		return CP56Time2a{}, fmt.Errorf("CP56Time2a contains an out-of-range field: %w", ErrInvalidFrame)
	}
	return timestamp, nil
}

func (timestamp CP56Time2a) Time(location *time.Location) (time.Time, error) {
	if timestamp.Invalid {
		return time.Time{}, errors.New("CP56Time2a timestamp is marked invalid")
	}
	if location == nil {
		return time.Time{}, errors.New("CP56Time2a requires an explicit location")
	}

	value := time.Date(2000+int(timestamp.Year), time.Month(timestamp.Month), int(timestamp.Day), int(timestamp.Hour), int(timestamp.Minute), int(timestamp.Milliseconds/1000), int(timestamp.Milliseconds%1000)*int(time.Millisecond), location)
	if value.Year() != 2000+int(timestamp.Year) || value.Month() != time.Month(timestamp.Month) || value.Day() != int(timestamp.Day) {
		return time.Time{}, errors.New("CP56Time2a contains an invalid calendar date")
	}
	return value, nil
}

type LinkageFrame struct {
	StationNo    uint16
	MonitorIndex uint16
	EventType    EventType
	Property     string
	Value        string
	Timestamp    CP56Time2a
}

func (frame LinkageFrame) PointKey() string {
	return fmt.Sprintf("%d_%d", frame.StationNo, frame.MonitorIndex)
}

func ParsePointTableFrame(packet []byte) (PointTableFrame, error) {
	if err := validateFrame(packet, TypePointTable, frameHeaderLen+2+1+1+4+2+checksumLen); err != nil {
		return PointTableFrame{}, err
	}
	if packet[7] > 1 {
		return PointTableFrame{}, fmt.Errorf("invalid point-table continuation flag %d: %w", packet[7], ErrInvalidFrame)
	}
	dataLength := int(binary.BigEndian.Uint16(packet[13:15]))
	if dataLength > pointTableDataMax {
		return PointTableFrame{}, fmt.Errorf("point-table data length %d exceeds %d: %w", dataLength, pointTableDataMax, ErrInvalidFrame)
	}
	if len(packet) != 15+dataLength+checksumLen {
		return PointTableFrame{}, fmt.Errorf("point-table frame length %d does not match data length %d: %w", len(packet), dataLength, ErrInvalidFrame)
	}

	frame := PointTableFrame{
		StationNo: binary.LittleEndian.Uint16(packet[5:7]),
		HasMore:   packet[7] == 1,
		Sequence:  packet[8],
		Data:      append([]byte(nil), packet[15:15+dataLength]...),
	}
	copy(frame.StartBytes[:], packet[9:13])
	return frame, nil
}

func ParseDevicePoints(data []byte) ([]DevicePoint, error) {
	decoded, err := simplifiedchinese.GBK.NewDecoder().Bytes(data)
	if err != nil {
		return nil, fmt.Errorf("decode device resource information: %w: %v", ErrInvalidText, err)
	}

	points := make([]DevicePoint, 0)
	for lineNumber, line := range strings.Split(string(decoded), "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if !strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 7 {
			return nil, fmt.Errorf("line %d has %d columns, want 7: %w", lineNumber+1, len(fields), ErrInvalidPointTable)
		}

		stationNo, err := strconv.ParseUint(strings.TrimSpace(fields[1]), 10, 16)
		if err != nil {
			return nil, fmt.Errorf("line %d has invalid station number: %w", lineNumber+1, ErrInvalidPointTable)
		}
		monitorIndex, err := strconv.ParseUint(strings.TrimSpace(fields[2]), 10, 16)
		if err != nil {
			return nil, fmt.Errorf("line %d has invalid monitor index: %w", lineNumber+1, ErrInvalidPointTable)
		}
		linkageText := strings.TrimSpace(fields[6])
		if linkageText != "是" && linkageText != "否" {
			return nil, fmt.Errorf("line %d has invalid linkage flag %q: %w", lineNumber+1, linkageText, ErrInvalidPointTable)
		}
		if strings.TrimSpace(fields[3]) == "" {
			return nil, fmt.Errorf("line %d has an empty device name: %w", lineNumber+1, ErrInvalidPointTable)
		}

		points = append(points, DevicePoint{
			StationNo:    uint16(stationNo),
			MonitorIndex: uint16(monitorIndex),
			DeviceName:   strings.TrimSpace(fields[3]),
			DeviceType:   strings.TrimSpace(fields[4]),
			PhysicalID:   strings.TrimSpace(fields[5]),
			Linkage:      linkageText == "是",
		})
	}
	if len(points) == 0 {
		return nil, fmt.Errorf("no point rows found: %w", ErrInvalidPointTable)
	}
	return points, nil
}

func ParseLinkageFrame(packet []byte) (LinkageFrame, error) {
	if err := validateFrame(packet, TypeLinkage, 5+2+2+1+1+1+7+1); err != nil {
		return LinkageFrame{}, err
	}
	attributeLength := int(packet[10])
	valueLengthOffset := 11 + attributeLength
	if valueLengthOffset >= len(packet)-1 {
		return LinkageFrame{}, fmt.Errorf("attribute length exceeds frame: %w", ErrInvalidFrame)
	}
	valueLength := int(packet[valueLengthOffset])
	timestampOffset := valueLengthOffset + 1 + valueLength
	if timestampOffset+7+checksumLen != len(packet) {
		return LinkageFrame{}, fmt.Errorf("linkage frame length does not match text fields: %w", ErrInvalidFrame)
	}
	if packet[9] > byte(EventMeasurement) {
		return LinkageFrame{}, fmt.Errorf("unsupported linkage event type %d: %w", packet[9], ErrInvalidFrame)
	}

	property, err := decodeText(packet[11:valueLengthOffset])
	if err != nil {
		return LinkageFrame{}, err
	}
	value, err := decodeText(packet[valueLengthOffset+1 : timestampOffset])
	if err != nil {
		return LinkageFrame{}, err
	}
	timestamp, err := ParseCP56Time2a(packet[timestampOffset : timestampOffset+7])
	if err != nil {
		return LinkageFrame{}, err
	}

	return LinkageFrame{
		StationNo:    binary.LittleEndian.Uint16(packet[5:7]),
		MonitorIndex: binary.LittleEndian.Uint16(packet[7:9]),
		EventType:    EventType(packet[9]),
		Property:     property,
		Value:        value,
		Timestamp:    timestamp,
	}, nil
}

func decodeText(data []byte) (string, error) {
	decoded, err := simplifiedchinese.GBK.NewDecoder().Bytes(data)
	if err != nil {
		return "", fmt.Errorf("decode protocol text: %w: %v", ErrInvalidText, err)
	}
	return string(bytes.TrimSpace(decoded)), nil
}

func validateFrame(packet []byte, expectedType byte, minimumLength int) error {
	if len(packet) < minimumLength {
		return fmt.Errorf("frame length %d is below minimum %d: %w", len(packet), minimumLength, ErrInvalidFrame)
	}
	if !bytes.Equal(packet[:4], framePreamble[:]) {
		return fmt.Errorf("frame preamble mismatch: %w", ErrInvalidFrame)
	}
	if packet[4] != expectedType {
		return fmt.Errorf("frame type 0x%02x does not match 0x%02x: %w", packet[4], expectedType, ErrInvalidFrame)
	}
	var checksum byte
	for _, value := range packet[5 : len(packet)-1] {
		checksum += value
	}
	if checksum != packet[len(packet)-1] {
		return fmt.Errorf("checksum is 0x%02x, want 0x%02x: %w", packet[len(packet)-1], checksum, ErrInvalidChecksum)
	}
	return nil
}
