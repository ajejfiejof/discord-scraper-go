package discord

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"github.com/pratherbytecraft/discord-scraper-go/pkg/snowflake"
)

// SearchFilter controls the advanced search endpoint.
// Mirrors Discrub's SearchCriteria plus sensible Go defaults.
type SearchFilter struct {
	AuthorIDs  []Snowflake
	MentionIDs []Snowflake
	ChannelIDs []Snowflake
	HasTypes   []string // e.g. "link", "embed", "file", "image", "video", "sound"
	Content    string
	BeforeDate *Snowflake // not used directly; max_id snowflake derived
	AfterDate  *Snowflake
	BeforeTime *int64 // unix milli, will be turned into max_id snowflake if AfterDate empty
	AfterTime  *int64
	Pinned     *bool
	NSFW       bool // include_nsfw=true always
}

// QueryParam constants mirroring Discrub enum QueryStringParam.
const (
	QueryBefore = "before"
	QueryAfter  = "after"
	QueryAround = "around"
)

// FetchUser fetches a user by ID.
func (c *Client) FetchUser(ctx context.Context, userID Snowflake) (*User, error) {
	var u User
	if err := c.do(ctx, http.MethodGet, "/users/"+userID, nil, nil, &u); err != nil {
		return nil, err
	}
	return &u, nil
}

// FetchGuilds lists guilds the token user is in (up to 200).
func (c *Client) FetchGuilds(ctx context.Context) ([]Guild, error) {
	var guilds []Guild
	if err := c.do(ctx, http.MethodGet, "/users/@me/guilds", nil, nil, &guilds); err != nil {
		return nil, err
	}
	return guilds, nil
}

// FetchGuild fetches a single guild's full object (requires guild ID; uses /guilds/:id).
// Note: the /users/@me/guilds only returns partial. For full guild we try /guilds/:id.
func (c *Client) FetchGuild(ctx context.Context, guildID Snowflake) (*Guild, error) {
	var g Guild
	if err := c.do(ctx, http.MethodGet, "/guilds/"+guildID, nil, nil, &g); err != nil {
		return nil, err
	}
	return &g, nil
}

// FetchGuildChannels fetches channels for a guild.
func (c *Client) FetchGuildChannels(ctx context.Context, guildID Snowflake) ([]Channel, error) {
	var chs []Channel
	if err := c.do(ctx, http.MethodGet, "/guilds/"+guildID+"/channels", nil, nil, &chs); err != nil {
		return nil, err
	}
	return chs, nil
}

// FetchChannel fetches a single channel.
func (c *Client) FetchChannel(ctx context.Context, channelID Snowflake) (*Channel, error) {
	var ch Channel
	if err := c.do(ctx, http.MethodGet, "/channels/"+channelID, nil, nil, &ch); err != nil {
		return nil, err
	}
	return &ch, nil
}

// FetchRoles lists roles for guild.
func (c *Client) FetchRoles(ctx context.Context, guildID Snowflake) ([]Role, error) {
	var roles []Role
	if err := c.do(ctx, http.MethodGet, "/guilds/"+guildID+"/roles", nil, nil, &roles); err != nil {
		return nil, err
	}
	return roles, nil
}

// FetchDMs lists DM channels.
func (c *Client) FetchDMs(ctx context.Context) ([]Channel, error) {
	var chs []Channel
	if err := c.do(ctx, http.MethodGet, "/users/@me/channels", nil, nil, &chs); err != nil {
		return nil, err
	}
	return chs, nil
}

// FetchMessages paginates channel messages.
// limit is capped to 100 (Discord max). lastID + dir controls pagination.
// dir is "before", "after", "around". Empty lastID fetches latest.
func (c *Client) FetchMessages(ctx context.Context, channelID, lastID, dir string, limit int) ([]Message, error) {
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	if dir == QueryAround {
		limit = 50 // Discord caps around at 50
	}
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	if lastID != "" && dir != "" {
		q.Set(dir, lastID)
	} else if lastID != "" {
		q.Set("before", lastID)
	}
	var msgs []Message
	if err := c.do(ctx, http.MethodGet, "/channels/"+channelID+"/messages", q, nil, &msgs); err != nil {
		return nil, err
	}
	return msgs, nil
}

// FetchAllMessages is a helper that paginates until exhaustion or ctx cancel.
// Calls yield for each batch.
func (c *Client) FetchAllMessages(ctx context.Context, channelID string, yield func([]Message) error) error {
	var before Snowflake
	for {
		msgs, err := c.FetchMessages(ctx, channelID, before, QueryBefore, 100)
		if err != nil {
			return err
		}
		if len(msgs) == 0 {
			return nil
		}
		if err := yield(msgs); err != nil {
			return err
		}
		if len(msgs) < 100 {
			return nil
		}
		before = msgs[len(msgs)-1].ID
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

// FetchArchivedPrivateThreads fetches archived private threads for a channel.
func (c *Client) FetchArchivedPrivateThreads(ctx context.Context, channelID Snowflake) ([]Channel, error) {
	// Discord returns {threads: [], members: [], has_more: bool}
	var resp struct {
		Threads []Channel `json:"threads"`
		HasMore bool      `json:"has_more"`
	}
	if err := c.do(ctx, http.MethodGet, "/channels/"+channelID+"/threads/archived/private", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Threads, nil
}

// FetchArchivedPublicThreads fetches archived public threads.
func (c *Client) FetchArchivedPublicThreads(ctx context.Context, channelID Snowflake) ([]Channel, error) {
	var resp struct {
		Threads []Channel `json:"threads"`
		HasMore bool      `json:"has_more"`
	}
	if err := c.do(ctx, http.MethodGet, "/channels/"+channelID+"/threads/archived/public", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Threads, nil
}

// FetchActiveThreads fetches active threads for guild (if guild-level) or channel.
func (c *Client) FetchActiveThreads(ctx context.Context, guildID Snowflake) ([]Channel, error) {
	var resp struct {
		Threads []Channel `json:"threads"`
	}
	// Guild active threads endpoint
	if err := c.do(ctx, http.MethodGet, "/guilds/"+guildID+"/threads/active", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Threads, nil
}

// FetchJoinedPrivateArchivedThreads for current user in channel.
func (c *Client) FetchJoinedPrivateArchivedThreads(ctx context.Context, channelID Snowflake) ([]Channel, error) {
	var resp struct {
		Threads []Channel `json:"threads"`
		HasMore bool      `json:"has_more"`
	}
	if err := c.do(ctx, http.MethodGet, "/channels/"+channelID+"/users/@me/threads/archived/private", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Threads, nil
}

// SearchMessages uses the search endpoint (guild or DM). Offset-based pagination, 25 per page.
// guildID may be empty for DM search (then channelID is required).
// filter may be nil for unfiltered dump.
func (c *Client) SearchMessages(ctx context.Context, guildID, channelID Snowflake, filter *SearchFilter, offset int) (*SearchResult, error) {
	q := url.Values{}
	// Build snowflake bounds from time if provided (Discrub behavior)
	if filter != nil {
		if filter.AfterTime != nil {
			// need time -> snowflake
			// snowflake expects time.Time, we approximate via millis
			// Use helper: reconstruct time from millis
			// Simpler: compute directly: (millis - epoch) <<22
			millis := *filter.AfterTime
			sf := strconv.FormatInt((millis-snowflake.DiscordEpoch)<<22, 10)
			q.Set("min_id", sf)
		}
		if filter.BeforeTime != nil {
			millis := *filter.BeforeTime
			sf := strconv.FormatInt((millis-snowflake.DiscordEpoch)<<22, 10)
			q.Set("max_id", sf)
		}
		if filter.Content != "" {
			q.Set("content", filter.Content)
		}
		if filter.Pinned != nil {
			if *filter.Pinned {
				q.Set("pinned", "true")
			} else {
				q.Set("pinned", "false")
			}
		}
		for _, id := range filter.AuthorIDs {
			q.Add("author_id", id)
		}
		for _, id := range filter.MentionIDs {
			q.Add("mentions", id)
		}
		for _, id := range filter.ChannelIDs {
			q.Add("channel_id", id)
		}
		for _, h := range filter.HasTypes {
			q.Add("has", h)
		}
		// DM case: channel_id already handled
		if filter.ChannelIDs == nil && channelID != "" && guildID == "" {
			// For DM search, channel_id param is still used via ChannelIDs, but if not set, use channelID
			// Actually Discord expects channel_id query param to limit DM search; handled via ChannelIDs
		}
	} else {
		// Default include_nsfw true (Discrub does)
	}
	// Always include_nsfw
	if q.Get("include_nsfw") == "" {
		q.Set("include_nsfw", "true")
	}
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}
	// Ensure min_id / max_id logic: if not set, omit (Discrub uses "null" then deletes)
	// We already omit when nil.

	var result SearchResult
	var path string
	if guildID != "" {
		path = "/guilds/" + guildID + "/messages/search"
	} else if channelID != "" {
		path = "/channels/" + channelID + "/messages/search"
	} else {
		return nil, ErrMissingTarget
	}
	if err := c.do(ctx, http.MethodGet, path, q, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Reactions: fetch users who reacted with emoji.
func (c *Client) FetchReactions(ctx context.Context, channelID, messageID Snowflake, emoji string, limit int, after Snowflake) ([]User, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	if after != "" {
		q.Set("after", after)
	}
	// Emoji must be URL-encoded (custom emoji id or unicode)
	// path: /channels/:id/messages/:id/reactions/:emoji
	// Use QueryEscape for emoji
	escaped := url.PathEscape(emoji)
	var users []User
	if err := c.do(ctx, http.MethodGet, "/channels/"+channelID+"/messages/"+messageID+"/reactions/"+escaped, q, nil, &users); err != nil {
		return nil, err
	}
	return users, nil
}

var ErrMissingTarget = &apiError{Status: 400, Body: "guildID and channelID both empty", URL: "search"}
