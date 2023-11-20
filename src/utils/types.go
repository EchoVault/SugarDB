package utils

type ContextServerID string
type ContextConnID string

type ApplyRequest struct {
	ServerID     string   `json:"ServerID"`
	ConnectionID string   `json:"ConnectionID"`
	CMD          []string `json:"CMD"`
}

type ApplyResponse struct {
	Error    error
	Response []byte
}
