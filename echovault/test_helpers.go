package echovault

import (
	"context"
	"github.com/echovault/echovault/internal"
	"github.com/echovault/echovault/internal/config"
)

func createEchoVault() *EchoVault {
	ev, _ := NewEchoVault(
		WithConfig(config.Config{
			DataDir: "",
		}),
	)
	return ev
}

func presetValue(server *EchoVault, ctx context.Context, key string, value interface{}) error {
	if err := server.setValues(ctx, map[string]interface{}{key: value}); err != nil {
		return err
	}
	return nil
}

func presetKeyData(server *EchoVault, ctx context.Context, key string, data internal.KeyData) {
	_ = server.setValues(ctx, map[string]interface{}{key: data.Value})
	server.setExpiry(ctx, key, data.ExpireAt, false)
}
