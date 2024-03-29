package echovault

import (
	"github.com/echovault/echovault/pkg/commands"
	"testing"
)

func presetValue(server *EchoVault, key string, value interface{}) {
	_, _ = server.CreateKeyAndLock(server.context, key)
	_ = server.SetValue(server.context, key, value)
	server.KeyUnlock(server.context, key)
}

func TestEchoVault_LLEN(t *testing.T) {
	server := NewEchoVault(
		WithCommands(commands.All()),
	)

	tests := []struct {
		preset      bool
		presetValue interface{}
		name        string
		key         string
		want        int
		wantErr     bool
	}{
		{ // If key exists and is a list, return the lists length
			preset:      true,
			key:         "key1",
			presetValue: []interface{}{"value1", "value2", "value3", "value4"},
			want:        4,
			wantErr:     false,
		},
		{ // If key does not exist, return 0
			preset:      false,
			key:         "key2",
			presetValue: nil,
			want:        0,
			wantErr:     false,
		},
		{ // Trying to get lengths on a non-list returns error
			preset:      true,
			key:         "key5",
			presetValue: "Default value",
			want:        0,
			wantErr:     true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.preset {
				presetValue(server, tt.key, tt.presetValue)
			}
			got, err := server.LLEN(tt.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("LLEN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("LLEN() got = %v, want %v", got, tt.want)
			}
		})
	}
}
