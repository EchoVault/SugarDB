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
	if _, err := server.CreateKeyAndLock(ctx, key); err != nil {
		return err
	}
	if err := server.SetValue(ctx, key, value); err != nil {
		return err
	}
	server.KeyUnlock(ctx, key)
	return nil
}

func presetKeyData(server *EchoVault, ctx context.Context, key string, data internal.KeyData) {
	_, _ = server.CreateKeyAndLock(ctx, key)
	defer server.KeyUnlock(ctx, key)
	_ = server.SetValue(ctx, key, data.Value)
	server.SetExpiry(ctx, key, data.ExpireAt, false)
}
