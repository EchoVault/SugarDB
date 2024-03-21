package acl

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"net"
	"slices"
	"testing"
)

var bindAddr string
var port uint16
var mockServer *server.Server

var acl *ACL

func init() {
	bindAddr = "localhost"
	port = 7490

	mockServer = setUpServer(bindAddr, port)

	go func() {
		mockServer.Start(context.Background())
	}()
}

func setUpServer(bindAddr string, port uint16) *server.Server {
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

	return server.NewServer(server.Opts{
		Config:   config,
		ACL:      acl,
		Commands: Commands(),
	})
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
	defer func() {
		_ = conn.Close()
	}()
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
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", bindAddr, port))
	if err != nil {
		t.Error(err)
	}
	defer func() {
		_ = conn.Close()
	}()
	r := resp.NewConn(conn)

	// Authenticate connection
	if err = r.WriteArray([]resp.Value{resp.StringValue("AUTH"), resp.StringValue("password1")}); err != nil {
		t.Error(err)
	}
	rv, _, err := r.ReadValue()
	if err != nil {
		t.Error(err)
	}
	if rv.String() != "OK" {
		t.Error("could not authenticate user")
	}

	// Since only ACL commands are loaded in this test suite, this test will only test against the
	// list of categories and commands available in the ACL module.
	tests := []struct {
		cmd     []resp.Value
		wantRes []string
		wantErr string
	}{
		{ // 1. Return list of categories
			cmd: []resp.Value{resp.StringValue("ACL"), resp.StringValue("CAT")},
			wantRes: []string{
				utils.ConnectionCategory,
				utils.SlowCategory,
				utils.FastCategory,
				utils.AdminCategory,
				utils.DangerousCategory,
			},
			wantErr: "",
		},
		{ // 2. Return list of commands in connection category
			cmd:     []resp.Value{resp.StringValue("ACL"), resp.StringValue("CAT"), resp.StringValue(utils.ConnectionCategory)},
			wantRes: []string{"auth"},
			wantErr: "",
		},
		{ // 3. Return list of commands in slow category
			cmd:     []resp.Value{resp.StringValue("ACL"), resp.StringValue("CAT"), resp.StringValue(utils.SlowCategory)},
			wantRes: []string{"auth", "acl|cat", "acl|users", "acl|setuser", "acl|getuser", "acl|deluser", "acl|list", "acl|load", "acl|save"},
			wantErr: "",
		},
		{ // 4. Return list of commands in fast category
			cmd:     []resp.Value{resp.StringValue("ACL"), resp.StringValue("CAT"), resp.StringValue(utils.FastCategory)},
			wantRes: []string{"acl|whoami"},
			wantErr: "",
		},
		{ // 5. Return list of commands in admin category
			cmd:     []resp.Value{resp.StringValue("ACL"), resp.StringValue("CAT"), resp.StringValue(utils.AdminCategory)},
			wantRes: []string{"acl|users", "acl|setuser", "acl|getuser", "acl|deluser", "acl|list", "acl|load", "acl|save"},
			wantErr: "",
		},
		{ // 6. Return list of commands in dangerous category
			cmd:     []resp.Value{resp.StringValue("ACL"), resp.StringValue("CAT"), resp.StringValue(utils.DangerousCategory)},
			wantRes: []string{"acl|users", "acl|setuser", "acl|getuser", "acl|deluser", "acl|list", "acl|load", "acl|save"},
			wantErr: "",
		},
		{ // 7. Return error when category does not exist
			cmd:     []resp.Value{resp.StringValue("ACL"), resp.StringValue("CAT"), resp.StringValue("non-existent")},
			wantRes: nil,
			wantErr: "Error category NON-EXISTENT not found",
		},
		{ // 8. Command too long
			cmd:     []resp.Value{resp.StringValue("ACL"), resp.StringValue("CAT"), resp.StringValue("category1"), resp.StringValue("category2")},
			wantRes: nil,
			wantErr: fmt.Sprintf("Error %s", utils.WrongArgsResponse),
		},
	}

	for _, test := range tests {
		if err = r.WriteArray(test.cmd); err != nil {
			t.Error(err)
		}
		rv, _, err = r.ReadValue()
		if err != nil {
			t.Error(err)
		}
		if test.wantErr != "" {
			if rv.Error().Error() != test.wantErr {
				t.Errorf("expected error response \"%s\", got \"%s\"", test.wantErr, rv.Error().Error())
			}
			continue
		}
		resArr := rv.Array()
		// Check if all the elements in the expected array are in the response array
		for _, expected := range test.wantRes {
			if !slices.ContainsFunc(resArr, func(value resp.Value) bool {
				return value.String() == expected
			}) {
				t.Errorf("could not find expected command \"%s\" in the response array for category", expected)
			}
		}
		// Check if all the elements in the response array are in the expected array
		for _, value := range resArr {
			if !slices.ContainsFunc(test.wantRes, func(expected string) bool {
				return value.String() == expected
			}) {
				t.Errorf("could not find response command \"%s\" in the expected array", value.String())
			}
		}
	}
}

func Test_HandleUsers(t *testing.T) {
	var port uint16 = 7491
	mockServer := setUpServer(bindAddr, port)
	go func() {
		mockServer.Start(context.Background())
	}()

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", bindAddr, port))
	if err != nil {
		t.Error(err)
	}
	defer func() {
		_ = conn.Close()
	}()

	r := resp.NewConn(conn)
	if err = r.WriteArray([]resp.Value{resp.StringValue("AUTH"), resp.StringValue("password1")}); err != nil {
		t.Error(err)
	}
	rv, _, err := r.ReadValue()
	if err != nil {
		t.Error(err)
	}
	if rv.String() != "OK" {
		t.Errorf("expected OK response, got \"%s\"", rv.String())
	}

	users := []string{"default", "with_password_user", "no_password_user", "disabled_user"}

	if err = r.WriteArray([]resp.Value{resp.StringValue("ACL"), resp.StringValue("USERS")}); err != nil {
		t.Error(err)
	}

	rv, _, err = r.ReadValue()
	if err != nil {
		t.Error(err)
	}

	resArr := rv.Array()

	// Check if all the expected users are in the response array
	for _, user := range users {
		if !slices.ContainsFunc(resArr, func(value resp.Value) bool {
			return value.String() == user
		}) {
			t.Errorf("could not find expected user \"%s\" in response array", user)
		}
	}

	// Check if all the users in the response array are in the expected users
	for _, value := range resArr {
		if !slices.ContainsFunc(users, func(user string) bool {
			return value.String() == user
		}) {
			t.Errorf("could not find response user \"%s\" in expected users array", value.String())
		}
	}
}

func Test_HandleSetUser(t *testing.T) {}

func Test_HandleGetUser(t *testing.T) {}

func Test_HandleDelUser(t *testing.T) {}

func Test_HandleWhoAmI(t *testing.T) {}

func Test_HandleList(t *testing.T) {}

func Test_HandleLoad(t *testing.T) {}

func Test_HandleSave(t *testing.T) {}
