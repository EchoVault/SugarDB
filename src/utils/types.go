package utils

type ApplyRequest struct {
	CMD []string `json:"CMD"`
}

type ApplyResponse struct {
	Error    error
	Response []byte
}
