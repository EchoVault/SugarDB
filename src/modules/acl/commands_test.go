package acl

import (
	"context"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"testing"
)

var bindAddr string
var port uint16
var mockServer *server.Server

var acl *ACL

func init() {
	bindAddr = "localhost"
	port = 7490

	config := utils.Config{
		BindAddr:       bindAddr,
		Port:           port,
		DataDir:        "",
		EvictionPolicy: utils.NoEviction,
	}

	acl = NewACL(config)

	mockServer = server.NewServer(server.Opts{
		Config: config,
		ACL:    acl,
	})

	go func() {
		mockServer.Start(context.Background())
	}()
}

func Test_HandleAuth(t *testing.T) {}

func Test_HandleCat(t *testing.T) {}

func Test_HandleUsers(t *testing.T) {}

func Test_HandleSetUser(t *testing.T) {}

func Test_HandleGetUser(t *testing.T) {}

func Test_HandleDelUser(t *testing.T) {}

func Test_HandleWhoAmI(t *testing.T) {}

func Test_HandleList(t *testing.T) {}

func Test_HandleLoad(t *testing.T) {}

func Test_HandleSave(t *testing.T) {}
