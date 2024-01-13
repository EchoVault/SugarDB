package ping

import (
	"bytes"
	"context"
	"fmt"
	server "github.com/echovault/echovault/src/mock/server"
	"github.com/echovault/echovault/src/utils"
	"testing"
)

func Test_HandlePing(t *testing.T) {
	ctx := context.Background()
	mockServer := &server.Server{}

	// Test PING with no string
	res, err := handlePing(ctx, []string{"PING"}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(res, []byte("+PONG\r\n\r\n")) {
		t.Errorf("expected %+v, got: %+v", "+PONG\r\n\r\n", res)
	}

	// Test PING with string arg
	testString := "Test String"
	res, err = handlePing(ctx, []string{"PING", testString}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}
	if !bytes.Equal(res, []byte(fmt.Sprintf("%s\r\n%d\r\n%s\r\n\r\n", "$", len(testString), testString))) {
		t.Errorf("expected: %+v, got: %+v", fmt.Sprintf("%s\r\n%d\r\n%s\r\n\r\n", "$", len(testString), testString), res)
	}

	// Test PING with more than 1 arg
	res, err = handlePing(ctx, []string{"PING", testString, testString}, mockServer, nil)
	if res != nil {
		t.Errorf("expected nil, got: %+v", res)
	}
	if err.Error() != utils.WRONG_ARGS_RESPONSE {
		t.Errorf("expected: %s, got: %s", utils.WRONG_ARGS_RESPONSE, err.Error())
	}
}
