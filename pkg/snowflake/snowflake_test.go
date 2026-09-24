package snowflake

import (
	"testing"
	"time"
)

func TestFromTimeAndToTime(t *testing.T) {
	// Discrub's generateSnowflake epoch check: 2023-01-01 should round-trip
	ts := time.Date(2023, 6, 15, 12, 0, 0, 0, time.UTC)
	sf := FromTime(ts)
	got, err := ToTime(sf)
	if err != nil {
		t.Fatalf("ToTime error: %v", err)
	}
	// Snowflake truncates to milliseconds then shifts, so we compare millis
	if got.UnixMilli() != ts.UnixMilli() {
		t.Fatalf("round-trip mismatch: got %d want %d", got.UnixMilli(), ts.UnixMilli())
	}
	// Known Discrub value: epoch itself should be 0
	epoch := time.UnixMilli(DiscordEpoch)
	if FromTime(epoch) != "0" {
		t.Fatalf("epoch snowflake should be 0, got %s", FromTime(epoch))
	}
}

func TestToTimeInvalid(t *testing.T) {
	if _, err := ToTime("not-a-number"); err == nil {
		t.Fatal("expected error for invalid snowflake")
	}
}
