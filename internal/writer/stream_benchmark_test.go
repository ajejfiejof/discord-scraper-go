package writer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pratherbytecraft/discord-scraper-go/internal/discord"
)

// generateBenchmarkBatch creates a realistic 100-message Discord JSON payload (~75 KiB).
func generateBenchmarkBatch() []byte {
	var buf bytes.Buffer
	buf.WriteString("[\n")
	for i := 0; i < 100; i++ {
		if i > 0 {
			buf.WriteString(",\n")
		}
		id := fmt.Sprintf("1234567890123456%02d", i)
		buf.WriteString(fmt.Sprintf(`{
			"id": "%s",
			"type": 0,
			"content": "This is benchmark message number %d with some text content and keywords",
			"channel_id": "999888777666555444",
			"author": {
				"id": "111222333444555666",
				"username": "benchmark_user_%d",
				"discriminator": "0",
				"avatar": null
			},
			"attachments": [],
			"embeds": [],
			"pinned": false,
			"timestamp": "2024-01-01T12:00:00.000000+00:00"
		}`, id, i, i%10))
	}
	buf.WriteString("\n]")
	return buf.Bytes()
}

// BenchmarkClassicReflectionPipeline measures the old Unmarshal -> Struct -> json.Encoder path.
func BenchmarkClassicReflectionPipeline(b *testing.B) {
	raw := generateBenchmarkBatch()
	dir := b.TempDir()
	w, err := New(dir, "jsonl")
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	ch := discord.Channel{ID: "bench_classic"}
	name := "bench"
	ch.Name = &name
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 1. Unmarshal into structs
		var msgs []discord.Message
		if err := json.Unmarshal(raw, &msgs); err != nil {
			b.Fatal(err)
		}
		// 2. Re-serialize each struct with json.Encoder
		if err := w.WriteChannel(ctx, "guild1", ch, msgs); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkZeroCopyStreamPipeline measures the new zero-copy stream writer.
func BenchmarkZeroCopyStreamPipeline(b *testing.B) {
	raw := generateBenchmarkBatch()
	dir := b.TempDir()
	w, err := New(dir, "jsonl")
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	ch := discord.Channel{ID: "bench_zerocopy"}
	name := "bench"
	ch.Name = &name
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _, err := w.WriteRawBatch(ctx, "guild1", ch, raw, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSparserFilterZeroCopy measures Sparser raw-byte filtering without unmarshaling.
func BenchmarkSparserFilterZeroCopy(b *testing.B) {
	raw := generateBenchmarkBatch()
	dir := b.TempDir()
	w, err := New(dir, "jsonl")
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	ch := discord.Channel{ID: "bench_sparser"}
	name := "bench"
	ch.Name = &name
	ctx := context.Background()
	filter := NewStreamFilter("number 42", "", "")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _, err := w.WriteRawBatch(ctx, "guild1", ch, raw, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}
