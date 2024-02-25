package admin

import (
	"bytes"
	"context"
	"fmt"
	"github.com/echovault/echovault/src/server"
	"github.com/tidwall/resp"
	"testing"
)

func Test_CommandsHandler(t *testing.T) {
	mockServer := server.NewServer(server.Opts{
		Commands: Commands(),
	})

	res, err := handleGetAllCommands(context.Background(), []string{"commands"}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}

	rd := resp.NewReader(bytes.NewReader(res))
	rv, _, err := rd.ReadValue()
	if err != nil {
		t.Error(err)
	}

	for _, element := range rv.Array() {
		fmt.Println(element)
	}
}
