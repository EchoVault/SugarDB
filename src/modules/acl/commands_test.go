package acl

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"net"
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
		RequirePass:    true,
		Password:       "password1",
	}

	acl = NewACL(config)
	acl.Users = append(acl.Users, generateInitialTestUsers()...)

	mockServer = server.NewServer(server.Opts{
		Config:   config,
		ACL:      acl,
		Commands: Commands(),
	})

	go func() {
		mockServer.Start(context.Background())
	}()
}

func generateInitialTestUsers() []*User {
	// User with both hash password and plaintext password
	withPasswordUser := CreateUser("with_password_user")
	h := sha256.New()
	h.Write([]byte("password3"))
	withPasswordUser.Passwords = []Password{
		{PasswordType: PasswordPlainText, PasswordValue: "password2"},
		{PasswordType: PasswordSHA256, PasswordValue: string(h.Sum(nil))},
	}

	// User with NoPassword option
	noPasswordUser := CreateUser("no_password_user")
	noPasswordUser.Passwords = []Password{
		{PasswordType: PasswordPlainText, PasswordValue: "password4"},
	}
	noPasswordUser.NoPassword = true

	// Disabled user
	disabledUser := CreateUser("disabled_user")
	disabledUser.Passwords = []Password{
		{PasswordType: PasswordPlainText, PasswordValue: "password5"},
	}
	disabledUser.Enabled = false

	return []*User{
		withPasswordUser,
		noPasswordUser,
		disabledUser,
	}
}

func Test_HandleAuth(t *testing.T) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", bindAddr, port))
	if err != nil {
		t.Error(err)
	}
	r := resp.NewConn(conn)

	tests := []struct {
		cmd     []resp.Value
		wantRes string
		wantErr string
	}{
		{ // 1. Authenticate with default user without specifying username
			cmd:     []resp.Value{resp.StringValue("AUTH"), resp.StringValue("password1")},
			wantRes: "OK",
			wantErr: "",
		},
		{ // 2. Authenticate with plaintext password
			cmd: []resp.Value{
				resp.StringValue("AUTH"),
				resp.StringValue("with_password_user"),
				resp.StringValue("password2"),
			},
			wantRes: "OK",
			wantErr: "",
		},
		{ // 3. Authenticate with SHA256 password
			cmd: []resp.Value{
				resp.StringValue("AUTH"),
				resp.StringValue("with_password_user"),
				resp.StringValue("password3"),
			},
			wantRes: "OK",
			wantErr: "",
		},
		{ // 4. Authenticate with no password user
			cmd: []resp.Value{
				resp.StringValue("AUTH"),
				resp.StringValue("no_password_user"),
				resp.StringValue("password4"),
			},
			wantRes: "OK",
			wantErr: "",
		},
		{ // 5. Fail to authenticate with disabled user
			cmd: []resp.Value{
				resp.StringValue("AUTH"),
				resp.StringValue("disabled_user"),
				resp.StringValue("password5"),
			},
			wantRes: "",
			wantErr: "Error user disabled_user is disabled",
		},
		{ // 6. Fail to authenticate with non-existent user
			cmd: []resp.Value{
				resp.StringValue("AUTH"),
				resp.StringValue("non_existent_user"),
				resp.StringValue("password6"),
			},
			wantRes: "",
			wantErr: "Error no user with username non_existent_user",
		},
		{ // 7. Command too short
			cmd:     []resp.Value{resp.StringValue("AUTH")},
			wantRes: "",
			wantErr: fmt.Sprintf("Error %s", utils.WrongArgsResponse),
		},
		{ // 8. Command too long
			cmd: []resp.Value{
				resp.StringValue("AUTH"),
				resp.StringValue("user"),
				resp.StringValue("password1"),
				resp.StringValue("password2"),
			},
			wantRes: "",
			wantErr: fmt.Sprintf("Error %s", utils.WrongArgsResponse),
		},
	}

	for _, test := range tests {
		if err = r.WriteArray(test.cmd); err != nil {
			t.Error(err)
		}
		rv, _, err := r.ReadValue()
		if err != nil {
			t.Error(err)
		}
		if test.wantErr != "" {
			if rv.Error().Error() != test.wantErr {
				t.Errorf("expected error response \"%s\", got \"%s\"", test.wantErr, rv.Error().Error())
			}
			continue
		}
		if rv.String() != test.wantRes {
			t.Errorf("expected response \"%s\", got \"%s\"", test.wantRes, rv.String())
		}
	}
}

func Test_HandleCat(t *testing.T) {
	// Since only ACL commands are loaded in this test suite, this test will only test against the
	// list of categories and commands available in the ACL module.
}

func Test_HandleUsers(t *testing.T) {}

func Test_HandleSetUser(t *testing.T) {}

func Test_HandleGetUser(t *testing.T) {}

func Test_HandleDelUser(t *testing.T) {}

func Test_HandleWhoAmI(t *testing.T) {}

func Test_HandleList(t *testing.T) {}

func Test_HandleLoad(t *testing.T) {}

func Test_HandleSave(t *testing.T) {}
