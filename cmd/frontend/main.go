package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	http.Handle("/", http.FileServer(http.Dir("frontend")))
	http.HandleFunc("/api/discord/channels", handleDiscordChannels)
	http.HandleFunc("/api/discord/messages", handleDiscordMessages)
	http.HandleFunc("/api/forum/topics", handleForumTopics)
	http.HandleFunc("/api/forum/posts", handleForumPosts)
	http.HandleFunc("/api/stats", handleStats)
	fmt.Println("Frontend on http://localhost:9979 (Discord) and http://localhost:9979/forum.html (Forum)")
	fmt.Println("API: /api/discord/channels, /api/discord/messages?channel=ID&q=&limit=50, /api/forum/topics, /api/forum/posts?topic=ID")
	log.Fatal(http.ListenAndServe(":9979", nil))
}

func handleDiscordChannels(w http.ResponseWriter, r *http.Request) {
	// Read channels.json for guild
	data, err := os.ReadFile("output/332269591378132992/channels.json")
	if err != nil {
		// Try generic output
		matches, _ := filepath.Glob("output/*/channels.json")
		if len(matches) > 0 {
			data, err = os.ReadFile(matches[0])
		}
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func handleDiscordMessages(w http.ResponseWriter, r *http.Request) {
	channel := r.URL.Query().Get("channel")
	q := strings.ToLower(r.URL.Query().Get("q"))
	limitStr := r.URL.Query().Get("limit")
	limit := 50
	if limitStr != "" {
		fmt.Sscanf(limitStr, "%d", &limit)
		if limit > 200 {
			limit = 200
		}
	}
	if channel == "" {
		http.Error(w, "channel required", 400)
		return
	}
	// Find file for channel
	matches, _ := filepath.Glob(fmt.Sprintf("output/*/%s*.jsonl", channel))
	if len(matches) == 0 {
		matches, _ = filepath.Glob(fmt.Sprintf("output/332269591378132992/%s*.jsonl", channel))
	}
	if len(matches) == 0 {
		http.Error(w, "channel not found", 404)
		return
	}
	f, err := os.Open(matches[0])
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer f.Close()
	var out []json.RawMessage
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 1024*1024)
	scanner.Buffer(buf, 10*1024*1024)
	for scanner.Scan() && len(out) < limit*5 {
		line := scanner.Bytes()
		if q != "" && !strings.Contains(strings.ToLower(string(line)), q) {
			continue
		}
		cp := make([]byte, len(line))
		copy(cp, line)
		out = append(out, cp)
		if len(out) >= limit {
			break
		}
	}
	// Reverse to show newest first? File is oldest first? Our writer appends in order fetched (newest first via before), so file is newest first.
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"messages": out, "count": len(out)})
}

func handleForumTopics(w http.ResponseWriter, r *http.Request) {
	q := strings.ToLower(r.URL.Query().Get("q"))
	data, err := os.ReadFile("forum-output/topics.json")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	var topics []map[string]any
	if err := json.Unmarshal(data, &topics); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	// Filter by q
	if q != "" {
		var filtered []map[string]any
		for _, t := range topics {
			title, _ := t["title"].(string)
			if strings.Contains(strings.ToLower(title), q) {
				filtered = append(filtered, t)
			}
		}
		topics = filtered
	}
	// Sort by posts_count desc
	sort.Slice(topics, func(i, j int) bool {
		a, _ := topics[i]["posts_count"].(float64)
		b, _ := topics[j]["posts_count"].(float64)
		return a > b
	})
	if len(topics) > 100 {
		topics = topics[:100]
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"topics": topics, "count": len(topics)})
}

func handleForumPosts(w http.ResponseWriter, r *http.Request) {
	topicID := r.URL.Query().Get("topic")
	if topicID == "" {
		http.Error(w, "topic required", 400)
		return
	}
	matches, _ := filepath.Glob(fmt.Sprintf("forum-output/topics/%s-*.json", topicID))
	if len(matches) == 0 {
		http.Error(w, "topic not found", 404)
		return
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	var topic map[string]any
	if err := json.Unmarshal(data, &topic); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(topic)
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	// Quick stats for both
	discordFiles, _ := filepath.Glob("output/*/*.jsonl")
	var discordMsgs int
	for _, f := range discordFiles {
		// Count via wc -l equivalent: count newlines
		b, _ := os.ReadFile(f)
		discordMsgs += strings.Count(string(b), "\n")
	}
	forumTopics := 0
	if data, err := os.ReadFile("forum-output/topics.json"); err == nil {
		var topics []any
		json.Unmarshal(data, &topics)
		forumTopics = len(topics)
	}
	forumUsers, _ := filepath.Glob("forum-output/users/*.json")
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"discord_msgs":   discordMsgs,
		"discord_files":  len(discordFiles),
		"forum_topics":   forumTopics,
		"forum_users":    len(forumUsers),
		"discord_size":   dirSize("output"),
		"forum_size":     dirSize("forum-output"),
	})
}

func dirSize(path string) string {
	var size int64
	filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	if size < 1024*1024 {
		return fmt.Sprintf("%.1fK", float64(size)/1024)
	}
	if size < 1024*1024*1024 {
		return fmt.Sprintf("%.1fM", float64(size)/1024/1024)
	}
	return fmt.Sprintf("%.2fG", float64(size)/1024/1024/1024)
}
