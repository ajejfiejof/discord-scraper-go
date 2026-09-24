package writer

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/floor"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/pratherbytecraft/discord-scraper-go/internal/discord"
)

// ParquetWriter wraps floor.Writer per channel for columnar output.
// Pure Go, SNAPPY compressed, 100k rows/row-group, queryable via DuckDB read_parquet.
type ParquetWriter struct {
	baseDir string
	mu      sync.Mutex
	files   map[string]*parquetFile
}

type parquetFile struct {
	f    *os.File
	fw   *goparquet.FileWriter
	w    *floor.Writer
	bw   *bufio.Writer // not used but kept for symmetry
	count int64
	mu   sync.Mutex
	path string
}

func NewParquetWriter(baseDir string) (*ParquetWriter, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}
	return &ParquetWriter{baseDir: baseDir, files: make(map[string]*parquetFile)}, nil
}

func (pw *ParquetWriter) key(guildID, channelID string) string {
	if guildID != "" {
		return guildID + "/" + channelID
	}
	return "dm/" + channelID
}

func (pw *ParquetWriter) ensureFile(guildID, channelID, channelName string) (*parquetFile, error) {
	k := pw.key(guildID, channelID)
	pw.mu.Lock()
	if pf, ok := pw.files[k]; ok {
		pw.mu.Unlock()
		return pf, nil
	}
	pw.mu.Unlock()

	dir := filepath.Join(pw.baseDir, safe(guildIDOrDM(guildID)))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	base := channelID
	if channelName != "" {
		base = fmt.Sprintf("%s-%s", channelID, safe(channelName))
	}
	path := filepath.Join(dir, base+".parquet")
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	bw := bufio.NewWriterSize(f, 256*1024)
	// floor writer needs goparquet.FileWriter with parquet-go options
	fw := goparquet.NewFileWriter(bw,
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
		goparquet.WithCreator("discord-scraper-go"),
		goparquet.WithMaxRowGroupSize(100*1024*1024), // 100MB row group
	)
	w := floor.NewWriter(fw)
	pf := &parquetFile{f: f, fw: fw, w: w, bw: bw, path: path}
	pw.mu.Lock()
	if existing, ok := pw.files[k]; ok {
		_ = f.Close()
		pw.mu.Unlock()
		return existing, nil
	}
	pw.files[k] = pf
	pw.mu.Unlock()
	return pf, nil
}

func (pw *ParquetWriter) WriteChannel(ctx context.Context, guildID string, ch discord.Channel, msgs []discord.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	name := ""
	if ch.Name != nil {
		name = *ch.Name
	}
	pf, err := pw.ensureFile(guildID, ch.ID, name)
	if err != nil {
		return err
	}
	pf.mu.Lock()
	defer pf.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	for _, m := range msgs {
		edited := ""
		if m.EditedTimestamp != nil {
			edited = *m.EditedTimestamp
		}
		row := ParquetRow{
			ID:              m.ID,
			ChannelID:       m.ChannelID,
			GuildID:         guildID,
			AuthorID:        m.Author.ID,
			AuthorUsername:  m.Author.Username,
			Content:         m.Content,
			Timestamp:       m.Timestamp,
			EditedTimestamp: edited,
			Pinned:          m.Pinned,
			Type:            int32(m.Type),
			HasAttachments:  len(m.Attachments) > 0,
			HasEmbeds:       len(m.Embeds) > 0,
		}
		if err := pf.w.Write(row); err != nil {
			return err
		}
	}
	pf.count += int64(len(msgs))
	return nil
}

func (pw *ParquetWriter) Close() error {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	var firstErr error
	for _, pf := range pw.files {
		pf.mu.Lock()
		if err := pf.fw.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := pf.bw.Flush(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := pf.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		pf.mu.Unlock()
	}
	return firstErr
}

func (pw *ParquetWriter) Stats() map[string]int64 {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	m := make(map[string]int64, len(pw.files))
	for k, pf := range pw.files {
		m[k] = pf.count
	}
	return m
}

var _ = context.Background
