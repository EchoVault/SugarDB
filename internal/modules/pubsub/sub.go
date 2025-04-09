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

package pubsub

import (
	"bufio"
	"bytes"
	"sync"
)

type EmbeddedSub struct {
	mux    sync.Mutex
	buff   *bytes.Buffer
	writer *bufio.Writer
	reader *bufio.Reader
}

func NewEmbeddedSub() *EmbeddedSub {
	sub := &EmbeddedSub{
		mux:  sync.Mutex{},
		buff: bytes.NewBuffer(make([]byte, 0)),
	}
	sub.writer = bufio.NewWriter(sub.buff)
	sub.reader = bufio.NewReader(sub.buff)
	return sub
}

func (sub *EmbeddedSub) Write(p []byte) (int, error) {
	sub.mux.Lock()
	defer sub.mux.Unlock()
	n, err := sub.writer.Write(p)
	if err != nil {
		return n, err
	}
	err = sub.writer.Flush()
	return n, err
}

func (sub *EmbeddedSub) Read(p []byte) (int, error) {
	sub.mux.Lock()
	defer sub.mux.Unlock()

	chunk, err := sub.reader.ReadBytes(byte('\n'))
	n := copy(p, chunk)

	return n, err
}
