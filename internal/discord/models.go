package discord

import "encoding/json"

// Snowflake is a Discord ID stored as string to preserve 64-bit precision.
type Snowflake = string

// User represents https://discord.com/developers/docs/resources/user#user-object
type User struct {
	ID            Snowflake `json:"id"`
	Username      string    `json:"username"`
	Discriminator string    `json:"discriminator"`
	GlobalName    *string   `json:"global_name"`
	Avatar        *string   `json:"avatar"`
	Bot           bool      `json:"bot,omitempty"`
	System        bool      `json:"system,omitempty"`
	PublicFlags   int       `json:"public_flags,omitempty"`
	Banner        *string   `json:"banner,omitempty"`
	AccentColor   *int      `json:"accent_color,omitempty"`
}

// Guild represents https://discord.com/developers/docs/resources/guild#guild-object
type Guild struct {
	ID                          Snowflake  `json:"id"`
	Name                        string     `json:"name"`
	Icon                        *string    `json:"icon"`
	IconHash                    *string    `json:"icon_hash"`
	Splash                      *string    `json:"splash"`
	DiscoverySplash             *string    `json:"discovery_splash"`
	OwnerID                     Snowflake  `json:"owner_id"`
	AFKChannelID                *Snowflake `json:"afk_channel_id"`
	AFKTimeout                  int        `json:"afk_timeout"`
	WidgetEnabled               *bool      `json:"widget_enabled"`
	WidgetChannelID             *Snowflake `json:"widget_channel_id"`
	VerificationLevel           int        `json:"verification_level"`
	DefaultMessageNotifications int        `json:"default_message_notifications"`
	ExplicitContentFilter       int        `json:"explicit_content_filter"`
	Roles                       []Role     `json:"roles,omitempty"`
	Emojis                      []Emoji    `json:"emojis,omitempty"`
	Features                    []string   `json:"features"`
	MFALevel                    int        `json:"mfa_level"`
	ApplicationID               *Snowflake `json:"application_id"`
	SystemChannelID             *Snowflake `json:"system_channel_id"`
	SystemChannelFlags          int        `json:"system_channel_flags"`
	RulesChannelID              *Snowflake `json:"rules_channel_id"`
	MaxPresences                *int       `json:"max_presences"`
	MaxMembers                  *int       `json:"max_members"`
	VanityURLCode               *string    `json:"vanity_url_code"`
	Description                 *string    `json:"description"`
	Banner                      *string    `json:"banner"`
	PremiumTier                 int        `json:"premium_tier"`
	PremiumSubscriptionCount    *int       `json:"premium_subscription_count"`
	PreferredLocale             string     `json:"preferred_locale"`
	NSFWLevel                   int        `json:"nsfw_level"`
	PremiumProgressBarEnabled   bool       `json:"premium_progress_bar_enabled"`
	ApproximateMemberCount      *int       `json:"approximate_member_count,omitempty"`
	ApproximatePresenceCount    *int       `json:"approximate_presence_count,omitempty"`
}

// Channel represents https://discord.com/developers/docs/resources/channel#channel-object
type Channel struct {
	ID                                   Snowflake  `json:"id"`
	Type                                 int        `json:"type"`
	GuildID                              *Snowflake `json:"guild_id,omitempty"`
	Position                             *int       `json:"position,omitempty"`
	PermissionOverwrites                 json.RawMessage `json:"permission_overwrites,omitempty"`
	Name                                 *string    `json:"name,omitempty"`
	Topic                                *string    `json:"topic,omitempty"`
	NSFW                                 *bool      `json:"nsfw,omitempty"`
	LastMessageID                        *Snowflake `json:"last_message_id,omitempty"`
	Bitrate                              *int       `json:"bitrate,omitempty"`
	UserLimit                            *int       `json:"user_limit,omitempty"`
	RateLimitPerUser                     *int       `json:"rate_limit_per_user,omitempty"`
	Recipients                           []User     `json:"recipients,omitempty"`
	Icon                                 *string    `json:"icon,omitempty"`
	OwnerID                              *Snowflake `json:"owner_id,omitempty"`
	ApplicationID                        *Snowflake `json:"application_id,omitempty"`
	Managed                              *bool      `json:"managed,omitempty"`
	ParentID                             *Snowflake `json:"parent_id,omitempty"`
	LastPinTimestamp                     *string    `json:"last_pin_timestamp,omitempty"`
	RTCRegion                            *string    `json:"rtc_region,omitempty"`
	VideoQualityMode                     *int       `json:"video_quality_mode,omitempty"`
	MessageCount                         *int       `json:"message_count,omitempty"`
	MemberCount                          *int       `json:"member_count,omitempty"`
	ThreadMetadata                       *ThreadMetadata `json:"thread_metadata,omitempty"`
	Member                               *ThreadMember   `json:"member,omitempty"`
	DefaultAutoArchiveDuration           *int       `json:"default_auto_archive_duration,omitempty"`
	Permissions                          *string    `json:"permissions,omitempty"`
	Flags                                *int       `json:"flags,omitempty"`
	TotalMessageSent                     *int       `json:"total_message_sent,omitempty"`
	AvailableTags                        json.RawMessage `json:"available_tags,omitempty"`
	AppliedTags                          []Snowflake `json:"applied_tags,omitempty"`
	DefaultReactionEmoji                 json.RawMessage `json:"default_reaction_emoji,omitempty"`
	DefaultThreadRateLimitPerUser        *int       `json:"default_thread_rate_limit_per_user,omitempty"`
	DefaultSortOrder                     *int       `json:"default_sort_order,omitempty"`
	DefaultForumLayout                   *int       `json:"default_forum_layout,omitempty"`
}

// ThreadMetadata https://discord.com/developers/docs/resources/channel#thread-metadata-object
type ThreadMetadata struct {
	Archived            bool    `json:"archived"`
	AutoArchiveDuration int     `json:"auto_archive_duration"`
	ArchiveTimestamp    string  `json:"archive_timestamp"`
	Locked              bool    `json:"locked,omitempty"`
	Invitable           *bool   `json:"invitable,omitempty"`
	CreateTimestamp     *string `json:"create_timestamp,omitempty"`
}

// ThreadMember https://discord.com/developers/docs/resources/channel#thread-member-object
type ThreadMember struct {
	ID            *Snowflake `json:"id,omitempty"`
	UserID        *Snowflake `json:"user_id,omitempty"`
	JoinTimestamp string     `json:"join_timestamp"`
	Flags         int        `json:"flags"`
}

// Message https://discord.com/developers/docs/resources/channel#message-object
type Message struct {
	ID                Snowflake       `json:"id"`
	ChannelID         Snowflake       `json:"channel_id"`
	Author            User            `json:"author"`
	Content           string          `json:"content"`
	Timestamp         string          `json:"timestamp"`
	EditedTimestamp   *string         `json:"edited_timestamp"`
	TTS               bool            `json:"tts"`
	MentionEveryone   bool            `json:"mention_everyone"`
	Mentions          []User          `json:"mentions"`
	MentionRoles      []Snowflake     `json:"mention_roles"`
	MentionChannels   json.RawMessage `json:"mention_channels,omitempty"`
	Attachments       []Attachment    `json:"attachments"`
	Embeds            []Embed         `json:"embeds"`
	Reactions         []Reaction      `json:"reactions,omitempty"`
	Nonce             json.RawMessage `json:"nonce,omitempty"`
	Pinned            bool            `json:"pinned"`
	WebhookID         *Snowflake      `json:"webhook_id,omitempty"`
	Type              int             `json:"type"`
	Activity          json.RawMessage `json:"activity,omitempty"`
	Application       json.RawMessage `json:"application,omitempty"`
	ApplicationID     *Snowflake      `json:"application_id,omitempty"`
	MessageReference  json.RawMessage `json:"message_reference,omitempty"`
	Flags             *int            `json:"flags,omitempty"`
	ReferencedMessage json.RawMessage `json:"referenced_message,omitempty"`
	Interaction       json.RawMessage `json:"interaction,omitempty"`
	Thread            *Channel        `json:"thread,omitempty"`
	Components        json.RawMessage `json:"components,omitempty"`
	StickerItems      json.RawMessage `json:"sticker_items,omitempty"`
	Stickers          json.RawMessage `json:"stickers,omitempty"`
	Position          *int            `json:"position,omitempty"`
	RoleSubscriptionData json.RawMessage `json:"role_subscription_data,omitempty"`
	Resolved          json.RawMessage `json:"resolved,omitempty"`
	Call              json.RawMessage `json:"call,omitempty"`
}

// Attachment https://discord.com/developers/docs/resources/channel#attachment-object
type Attachment struct {
	ID          Snowflake `json:"id"`
	Filename    string    `json:"filename"`
	Description *string   `json:"description,omitempty"`
	ContentType *string   `json:"content_type,omitempty"`
	Size        int       `json:"size"`
	URL         string    `json:"url"`
	ProxyURL    string    `json:"proxy_url"`
	Height      *int      `json:"height,omitempty"`
	Width       *int      `json:"width,omitempty"`
	Ephemeral   *bool     `json:"ephemeral,omitempty"`
	DurationSecs *float64 `json:"duration_secs,omitempty"`
	Waveform    *string   `json:"waveform,omitempty"`
}

// Embed https://discord.com/developers/docs/resources/channel#embed-object
type Embed struct {
	Title       *string         `json:"title,omitempty"`
	Type        *string         `json:"type,omitempty"`
	Description *string         `json:"description,omitempty"`
	URL         *string         `json:"url,omitempty"`
	Timestamp   *string         `json:"timestamp,omitempty"`
	Color       *int            `json:"color,omitempty"`
	Footer      json.RawMessage `json:"footer,omitempty"`
	Image       json.RawMessage `json:"image,omitempty"`
	Thumbnail   json.RawMessage `json:"thumbnail,omitempty"`
	Video       json.RawMessage `json:"video,omitempty"`
	Provider    json.RawMessage `json:"provider,omitempty"`
	Author      json.RawMessage `json:"author,omitempty"`
	Fields      json.RawMessage `json:"fields,omitempty"`
}

// Reaction https://discord.com/developers/docs/resources/channel#reaction-object
type Reaction struct {
	Count        int   `json:"count"`
	CountDetails json.RawMessage `json:"count_details,omitempty"`
	Me           bool  `json:"me"`
	MeBurst      bool  `json:"me_burst,omitempty"`
	Emoji        Emoji `json:"emoji"`
	BurstColors  []string `json:"burst_colors,omitempty"`
}

// Role https://discord.com/developers/docs/topics/permissions#role-object
type Role struct {
	ID          Snowflake `json:"id"`
	Name        string    `json:"name"`
	Color       int       `json:"color"`
	Hoist       bool      `json:"hoist"`
	Icon        *string   `json:"icon,omitempty"`
	UnicodeEmoji *string  `json:"unicode_emoji,omitempty"`
	Position    int       `json:"position"`
	Permissions string    `json:"permissions"`
	Managed     bool      `json:"managed"`
	Mentionable bool      `json:"mentionable"`
	Flags       int       `json:"flags"`
	Tags        json.RawMessage `json:"tags,omitempty"`
}

// Emoji https://discord.com/developers/docs/resources/emoji#emoji-object
type Emoji struct {
	ID            *Snowflake `json:"id"`
	Name          *string    `json:"name"`
	Roles         []Snowflake `json:"roles,omitempty"`
	User          *User      `json:"user,omitempty"`
	RequireColons *bool      `json:"require_colons,omitempty"`
	Managed       *bool      `json:"managed,omitempty"`
	Animated      *bool      `json:"animated,omitempty"`
	Available     *bool      `json:"available,omitempty"`
}

// SearchResult mirrors Discord's search response: messages is [][]Message, threads, total_results
type SearchResult struct {
	Messages      [][]Message `json:"messages"`
	Threads       []Channel   `json:"threads"`
	TotalResults  int         `json:"total_results"`
}

// GuildMember https://discord.com/developers/docs/resources/guild#guild-member-object
type GuildMember struct {
	User                       *User       `json:"user,omitempty"`
	Nick                       *string     `json:"nick"`
	Avatar                     *string     `json:"avatar"`
	Roles                      []Snowflake `json:"roles"`
	JoinedAt                   string      `json:"joined_at"`
	PremiumSince               *string     `json:"premium_since"`
	Deaf                       bool        `json:"deaf"`
	Mute                       bool        `json:"mute"`
	Flags                      int         `json:"flags"`
	Pending                    *bool       `json:"pending,omitempty"`
	Permissions                *string     `json:"permissions,omitempty"`
	CommunicationDisabledUntil *string     `json:"communication_disabled_until,omitempty"`
}

// ChannelType constants
const (
	ChannelTypeGuildText          = 0
	ChannelTypeDM                 = 1
	ChannelTypeGuildVoice         = 2
	ChannelTypeGroupDM            = 3
	ChannelTypeGuildCategory      = 4
	ChannelTypeGuildAnnouncement  = 5
	ChannelTypeAnnouncementThread = 10
	ChannelTypePublicThread       = 11
	ChannelTypePrivateThread      = 12
	ChannelTypeGuildStageVoice    = 13
	ChannelTypeGuildDirectory     = 14
	ChannelTypeGuildForum         = 15
	ChannelTypeGuildMedia         = 16
)

// IsTextLike returns true if channel can contain messages worth scraping.
// Mirrors Discrub's reasonable set: text, announcement, forum, media, threads, DMs.
func IsTextLike(t int) bool {
	switch t {
	case ChannelTypeGuildText, ChannelTypeDM, ChannelTypeGuildVoice,
		ChannelTypeGroupDM, ChannelTypeGuildAnnouncement,
		ChannelTypeAnnouncementThread, ChannelTypePublicThread, ChannelTypePrivateThread,
		ChannelTypeGuildForum, ChannelTypeGuildMedia, ChannelTypeGuildStageVoice:
		return true
	default:
		return false
	}
}

// IsThread returns true for thread channel types.
func IsThread(t int) bool {
	return t == ChannelTypeAnnouncementThread || t == ChannelTypePublicThread || t == ChannelTypePrivateThread
}
