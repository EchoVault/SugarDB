package ping

import (
	"context"
	"github.com/echovault/echovault/src/utils"
	"net"
	"reflect"
	"testing"
)

type args struct {
	ctx    context.Context
	cmd    []string
	server utils.Server
	conn   *net.Conn
}

func Test_handlePing(t *testing.T) {
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := handlePing(tt.args.ctx, tt.args.cmd, tt.args.server, tt.args.conn)
			if (err != nil) != tt.wantErr {
				t.Errorf("handlePing() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("handlePing() got = %v, want %v", got, tt.want)
			}
		})
	}
}
