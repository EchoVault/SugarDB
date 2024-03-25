// Copyright 2024 Kelvin Clement Mwinuka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package admin

import (
	"bytes"
	"context"
	"fmt"
	"github.com/echovault/echovault/src/server"
	"github.com/echovault/echovault/src/utils"
	"github.com/tidwall/resp"
	"testing"
)

func Test_CommandsHandler(t *testing.T) {
	mockServer := server.NewEchoVault(server.Opts{
		Config: utils.Config{
			DataDir:        "",
			EvictionPolicy: utils.NoEviction,
		},
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
