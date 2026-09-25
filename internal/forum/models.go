package forum

import "encoding/json"

// Minimal Discourse models for scraping — we keep raw JSON for flexibility, only type the fields we need for pagination.

type CategoriesResponse struct {
	CategoryList struct {
		Categories []Category `json:"categories"`
	} `json:"category_list"`
}

type Category struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Slug string `json:"slug"`
}

type TopicListResponse struct {
	TopicList struct {
		Topics []Topic `json:"topics"`
		MoreTopicsURL string `json:"more_topics_url"`
	} `json:"topic_list"`
	Users []User `json:"users"`
}

type Topic struct {
	ID               int             `json:"id"`
	Title            string          `json:"title"`
	Slug             string          `json:"slug"`
	PostsCount       int             `json:"posts_count"`
	ReplyCount       int             `json:"reply_count"`
	HighestPostNumber int            `json:"highest_post_number"`
	CreatedAt        string          `json:"created_at"`
	LastPostedAt     string          `json:"last_posted_at"`
	CategoryID       int             `json:"category_id"`
	Tags             json.RawMessage `json:"tags"`
	Views            int             `json:"views"`
	LikeCount        int             `json:"like_count"`
}

type TopicResponse struct {
	ID    int    `json:"id"`
	Title string `json:"title"`
	PostsCount int `json:"posts_count"`
	PostStream struct {
		Posts []Post `json:"posts"`
		Stream []int `json:"stream"`
	} `json:"post_stream"`
}

type PostsResponse struct {
	PostStream struct {
		Posts []Post `json:"posts"`
	} `json:"post_stream"`
	ID int `json:"id"`
}

type Post struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	Username   string `json:"username"`
	Cooked     string `json:"cooked"` // HTML
	Raw        string `json:"raw"`    // markdown source if available
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
	PostNumber int    `json:"post_number"`
	PostType   int    `json:"post_type"`
	ReplyToPostNumber *int `json:"reply_to_post_number"`
	Reads      int `json:"reads"`
	Score      float64 `json:"score"`
}

type User struct {
	ID       int    `json:"id"`
	Username string `json:"username"`
	Name     string `json:"name"`
	AvatarTemplate string `json:"avatar_template"`
	TrustLevel int `json:"trust_level"`
}
