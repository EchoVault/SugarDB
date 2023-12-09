package utils

import (
	"context"
	"net"
)

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

type Command struct {
	Command              string   `json:"Command"`
	Categories           []string `json:"Categories"`
	Description          string   `json:"Description"`
	HandleWithConnection bool     `json:"HandleWithConnection"`
	Sync                 bool     `json:"Sync"` // Specifies if command should be synced across cluster
	Plugin               Plugin
}

type Plugin interface {
	Name() string
	Commands() ([]byte, error)
	Description() string
	HandleCommand(ctx context.Context, cmd []string, server interface{}) ([]byte, error)
	HandleCommandWithConnection(ctx context.Context, cmd []string, server interface{}, conn *net.Conn) ([]byte, error)
}
