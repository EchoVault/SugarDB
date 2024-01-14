package etc

import (
	"context"
	"github.com/echovault/echovault/src/mock/server"
	"github.com/echovault/echovault/src/utils"
	"testing"
)

func Test_HandleSET(t *testing.T) {
	mockServer := server.NewMockServer()
	// Test set string
	res, err := handleSet(context.Background(), []string{"SET", "test", "Test_HandleSET"}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}
	vs, ok := mockServer.GetValue("test").(string)
	if !ok {
		t.Errorf("value not string")
	}
	if vs != "Test_HandleSET" {
		t.Errorf("expected value %s, got %s", "Test_HandleSET", vs)
	}
	// Test set integer
	res, err = handleSet(context.Background(), []string{"SET", "integer", "1245678910"}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}
	vi, ok := mockServer.GetValue("integer").(int)
	if !ok {
		t.Errorf("value not integer")
	}
	if vi != 1245678910 {
		t.Errorf("expected value %d, got %d", 1245678910, vi)
	}
	// Test set float
	res, err = handleSet(context.Background(), []string{"SET", "float", "45782.11341"}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}
	vf, ok := mockServer.GetValue("float").(float64)
	if !ok {
		t.Errorf("value not float")
	}
	if vf != 45782.11341 {
		t.Errorf("expected value %f, got %f", 45782.11341, vf)
	}
	// Test too few args
	res, err = handleSet(context.Background(), []string{"SET"}, mockServer, nil)
	if res != nil {
		t.Errorf("expected nil response, got: %+v", res)
	}
	if err.Error() != utils.WRONG_ARGS_RESPONSE {
		t.Errorf("expected error %s, got: %s", utils.WRONG_ARGS_RESPONSE, err.Error())
	}
	// Test too many args
	res, err = handleSet(context.Background(), []string{"SET", "name", "test", "1", "2", "3"}, mockServer, nil)
	if res != nil {
		t.Errorf("expected nil response, got: %+v", res)
	}
	if err.Error() != utils.WRONG_ARGS_RESPONSE {
		t.Errorf("expected error %s, got: %s", utils.WRONG_ARGS_RESPONSE, err.Error())
	}
}

func Test_HandleSETNX(t *testing.T) {
	mockerServer := server.NewMockServer()
	res, err := handleSetNX(context.Background(), []string{"SET", "test", "Test_HandleSETNX"}, mockerServer, nil)
	if err != nil {
		t.Error(err)
	}
	// Try to set existing key again
	res, err = handleSetNX(context.Background(), []string{"SET", "test", "Test_HandleSETNX_2"}, mockerServer, nil)
	if res != nil {
		t.Errorf("exptected nil response, got: %+v", res)
	}
	if err.Error() != "key test already exists" {
		t.Errorf("expected key test already exists, got %s", err.Error())
	}
}

func Test_HandleMSET(t *testing.T) {
	mockServer := server.NewMockServer()
	// Test multiple valid args
	res, err := handleMSet(context.Background(), []string{"SET", "test1", "value1", "test2", "10", "test3", "3.142"}, mockServer, nil)
	if err != nil {
		t.Error(err)
	}
	vs, ok := mockServer.GetValue("test1").(string)
	if !ok {
		t.Error("expected string value for test1 key")
	}
	if vs != "value1" {
		t.Errorf("expected value1, got: %s", vs)
	}
	vi, ok := mockServer.GetValue("test2").(int)
	if !ok {
		t.Error("expected int value for test2 key")
	}
	if vi != 10 {
		t.Errorf("expected 10, got: %d", vi)
	}
	vf, ok := mockServer.GetValue("test3").(float64)
	if vf != 3.142 {
		t.Errorf("expected 3.142, got: %f", vf)
	}
	if !ok {
		t.Errorf("expected float64 for test3 key")
	}

	// Test invalid number of args
	res, err = handleMSet(context.Background(), []string{"SET", "test1", "value1", "test2", "10", "test3"}, mockServer, nil)
	if res != nil {
		t.Errorf("expected nil response, got: %+v", res)
	}
	if err.Error() != "each key must have a matching value" {
		t.Errorf("expected errpr 'each key must have a matching value', got: %s", err.Error())
	}
}
