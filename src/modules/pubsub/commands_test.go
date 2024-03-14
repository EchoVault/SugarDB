package pubsub

import (
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"testing"
)

var mockServer *server.Server

func init() {
	mockServer = server.NewServer(server.Opts{
		Config: utils.Config{
			DataDir:        "",
			EvictionPolicy: utils.NoEviction,
		},
	})
}

func Test_HandleSubscribe(t *testing.T) {

}

func Test_HandleUnsubscribe(t *testing.T) {

}

func Test_HandlePublish(t *testing.T) {

}

func Test_HandlePubSubChannels(t *testing.T) {

}

func Test_HandleNumPat(t *testing.T) {

}

func Test_HandleNumSub(t *testing.T) {

}
