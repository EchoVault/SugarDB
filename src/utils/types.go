package utils

import (
	"context"
	"net"
)

type Server interface {
	KeyLock(ctx context.Context, key string) (bool, error)
	KeyUnlock(key string)
	KeyRLock(ctx context.Context, key string) (bool, error)
	KeyRUnlock(key string)
	KeyExists(key string) bool
	CreateKeyAndLock(ctx context.Context, key string) (bool, error)
	GetValue(key string) interface{}
	SetValue(ctx context.Context, key string, value interface{})
}

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

type SubCommand struct {
	SubCommand  string
	Description string
}

type Command struct {
	Command              string
	Categories           []string
	Description          string
	SubCommands          []SubCommand
	HandleWithConnection bool
	Sync                 bool // Specifies if command should be synced across cluster
}

type Plugin interface {
	Name() string
	Commands() ([]byte, error)
	Description() string
	HandleCommand(ctx context.Context, cmd []string, server Server) ([]byte, error)
	HandleCommandWithConnection(ctx context.Context, cmd []string, server Server, conn *net.Conn) ([]byte, error)
}
