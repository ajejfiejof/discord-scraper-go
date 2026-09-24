package snowflake

import (
	"strconv"
	"time"
)

// DiscordEpoch is 2015-01-01T00:00:00.000Z in milliseconds.
const DiscordEpoch int64 = 1420070400000

// FromTime generates a Discord snowflake string for a given time.
// Mirrors Discrub's generateSnowflake: ((millis - epoch) << 22)
func FromTime(t time.Time) string {
	millis := t.UnixMilli()
	id := (millis - DiscordEpoch) << 22
	return strconv.FormatInt(id, 10)
}

// ToTime extracts the creation timestamp from a snowflake.
func ToTime(snowflake string) (time.Time, error) {
	id, err := strconv.ParseInt(snowflake, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	millis := (id >> 22) + DiscordEpoch
	return time.UnixMilli(millis), nil
}

// MustTime panics on error (convenience for tests).
func MustTime(s string) time.Time {
	t, err := ToTime(s)
	if err != nil {
		panic(err)
	}
	return t
}
