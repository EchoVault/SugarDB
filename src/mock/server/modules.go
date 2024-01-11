package server

import (
	"context"
	"github.com/echovault/echovault/src/utils"
)

func (server *Server) GetAllCommands(ctx context.Context) []utils.Command {
	return []utils.Command{}
}

func (server *Server) GetACL() interface{} {
	return nil
}

func (server *Server) GetPubSub() interface{} {
	return nil
}
