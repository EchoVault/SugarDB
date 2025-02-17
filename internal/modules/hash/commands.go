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

package hash

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/echovault/sugardb/internal"
	"github.com/echovault/sugardb/internal/constants"
)

func handleHSET(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hsetKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]
	entries := Hash{}

	if len(params.Command[2:])%2 != 0 {
		return nil, errors.New("each field must have a corresponding value")
	}

	for i := 2; i <= len(params.Command)-2; i += 2 {
		k := params.Command[i]
		entries[k] = HashValue{Value: internal.AdaptType(params.Command[i+1])}
	}

	if !keyExists {
		if err = params.SetValues(params.Context, map[string]interface{}{key: entries}); err != nil {
			return nil, err
		}
		return []byte(fmt.Sprintf(":%d\r\n", len(entries))), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		// Not hash, save the entries map directly.
		if err = params.SetValues(params.Context, map[string]interface{}{key: entries}); err != nil {
			return nil, err
		}
		return []byte(fmt.Sprintf(":%d\r\n", len(entries))), nil
	}

	count := 0
	switch strings.ToLower(params.Command[0]) {
	case "hsetnx":
		// Handle HSETNX
		for field, _ := range entries {
			if _, ok := hash[field]; !ok {
				count += 1
			}
		}

		for field, value := range hash {
			entries[field] = value
		}
	default:
		// Handle HSET
		for field, value := range hash {
			if entries[field].Value == nil {
				entries[field] = HashValue{Value: value}
			}
		}
		count = len(entries)
	}

	if err = params.SetValues(params.Context, map[string]interface{}{key: entries}); err != nil {
		return nil, err
	}

	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

func handleHGET(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hgetKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]
	fields := params.Command[2:]

	if !keyExists {
		return []byte("$-1\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	var value HashValue

	res := fmt.Sprintf("*%d\r\n", len(fields))
	for _, field := range fields {
		value = hash[field]
		if value.Value == nil {
			res += "$-1\r\n"
			continue
		}
		if s, ok := value.Value.(string); ok {
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
			continue
		}
		if d, ok := value.Value.(int); ok {
			res += fmt.Sprintf(":%d\r\n", d)
			continue
		}
		if f, ok := value.Value.(float64); ok {
			fs := strconv.FormatFloat(f, 'f', -1, 64)
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(fs), fs)
			continue
		}
		res += fmt.Sprintf("$-1\r\n")
	}

	return []byte(res), nil
}

func handleHMGET(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hmgetKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]
	if !keyExists {
		return []byte("$-1\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	fields := params.Command[2:]

	var value HashValue

	res := fmt.Sprintf("*%d\r\n", len(fields))
	for _, field := range fields {
		value, ok = hash[field]
		if !ok {
			res += "$-1\r\n"
			continue
		}

		if s, ok := value.Value.(string); ok {
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
			continue
		}
		if d, ok := value.Value.(int); ok {
			res += fmt.Sprintf(":%d\r\n", d)
			continue
		}
		if f, ok := value.Value.(float64); ok {
			fs := strconv.FormatFloat(f, 'f', -1, 64)
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(fs), fs)
			continue
		}
		res += fmt.Sprintf("$-1\r\n")

	}
	return []byte(res), nil
}

func handleHSTRLEN(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hstrlenKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]
	fields := params.Command[2:]

	if !keyExists {
		return []byte("$-1\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	var value HashValue

	res := fmt.Sprintf("*%d\r\n", len(fields))
	for _, field := range fields {
		value = hash[field]
		if value.Value == nil {
			res += ":0\r\n"
			continue
		}
		if s, ok := value.Value.(string); ok {
			res += fmt.Sprintf(":%d\r\n", len(s))
			continue
		}
		if f, ok := value.Value.(float64); ok {
			fs := strconv.FormatFloat(f, 'f', -1, 64)
			res += fmt.Sprintf(":%d\r\n", len(fs))
			continue
		}
		if d, ok := value.Value.(int); ok {
			res += fmt.Sprintf(":%d\r\n", len(strconv.Itoa(d)))
			continue
		}
		res += ":0\r\n"
	}

	return []byte(res), nil
}

func handleHVALS(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hvalsKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]

	if !keyExists {
		return []byte("*0\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	res := fmt.Sprintf("*%d\r\n", len(hash))
	for _, val := range hash {
		if s, ok := val.Value.(string); ok {
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
			continue
		}
		if f, ok := val.Value.(float64); ok {
			fs := strconv.FormatFloat(f, 'f', -1, 64)
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(fs), fs)
			continue
		}
		if d, ok := val.Value.(int); ok {
			res += fmt.Sprintf(":%d\r\n", d)
		}
	}

	return []byte(res), nil
}

func handleHRANDFIELD(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hrandfieldKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]

	count := 1
	if len(params.Command) >= 3 {
		c, err := strconv.Atoi(params.Command[2])
		if err != nil {
			return nil, errors.New("count must be an integer")
		}
		if c == 0 {
			return []byte("*0\r\n"), nil
		}
		count = c
	}

	withvalues := false
	if len(params.Command) == 4 {
		if strings.EqualFold(params.Command[3], "withvalues") {
			withvalues = true
		} else {
			return nil, errors.New("result modifier must be withvalues")
		}
	}

	if !keyExists {
		return []byte("*0\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	// If count is the >= hash length, then return the entire hash
	if count >= len(hash) {
		res := fmt.Sprintf("*%d\r\n", len(hash))
		if withvalues {
			res = fmt.Sprintf("*%d\r\n", len(hash)*2)
		}
		for field, value := range hash {
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(field), field)
			if withvalues {
				if s, ok := value.Value.(string); ok {
					res += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
					continue
				}
				if f, ok := value.Value.(float64); ok {
					fs := strconv.FormatFloat(f, 'f', -1, 64)
					res += fmt.Sprintf("$%d\r\n%s\r\n", len(fs), fs)
					continue
				}
				if d, ok := value.Value.(int); ok {
					res += fmt.Sprintf(":%d\r\n", d)
					continue
				}
			}
		}
		return []byte(res), nil
	}

	// Get all the fields
	var fields []string
	for field, _ := range hash {
		fields = append(fields, field)
	}

	// Pluck fields and return them
	var pluckedFields []string
	var n int
	for i := 0; i < internal.AbsInt(count); i++ {
		n = rand.Intn(len(fields))
		pluckedFields = append(pluckedFields, fields[n])
		// If count is positive, remove the current field from list of fields
		if count > 0 {
			fields = slices.DeleteFunc(fields, func(s string) bool {
				return s == fields[n]
			})
		}
	}

	res := fmt.Sprintf("*%d\r\n", len(pluckedFields))
	if withvalues {
		res = fmt.Sprintf("*%d\r\n", len(pluckedFields)*2)
	}
	for _, field := range pluckedFields {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(field), field)
		if withvalues {
			if s, ok := hash[field].Value.(string); ok {
				res += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
				continue
			}
			if f, ok := hash[field].Value.(float64); ok {
				fs := strconv.FormatFloat(f, 'f', -1, 64)
				res += fmt.Sprintf("$%d\r\n%s\r\n", len(fs), fs)
				continue
			}
			if d, ok := hash[field].Value.(int); ok {
				res += fmt.Sprintf(":%d\r\n", d)
				continue
			}
		}
	}

	return []byte(res), nil
}

func handleHLEN(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hlenKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]

	if !keyExists {
		return []byte(":0\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	return []byte(fmt.Sprintf(":%d\r\n", len(hash))), nil
}

func handleHKEYS(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hkeysKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]

	if !keyExists {
		return []byte("*0\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	res := fmt.Sprintf("*%d\r\n", len(hash))
	for field, _ := range hash {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(field), field)
	}

	return []byte(res), nil
}

func handleHINCRBY(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hincrbyKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]
	field := params.Command[2]

	var intIncrement int
	var floatIncrement float64

	if strings.EqualFold(params.Command[0], "hincrbyfloat") {
		f, err := strconv.ParseFloat(params.Command[3], 64)
		if err != nil {
			return nil, errors.New("increment must be a float")
		}
		floatIncrement = f
	} else {
		i, err := strconv.Atoi(params.Command[3])
		if err != nil {
			return nil, errors.New("increment must be an integer")
		}
		intIncrement = i
	}

	if !keyExists {
		hash := make(Hash)
		if strings.EqualFold(params.Command[0], "hincrbyfloat") {
			hash[field] = HashValue{Value: floatIncrement}
			if err = params.SetValues(params.Context, map[string]interface{}{key: hash}); err != nil {
				return nil, err
			}
			return []byte(fmt.Sprintf("+%s\r\n", strconv.FormatFloat(floatIncrement, 'f', -1, 64))), nil
		} else {
			hash[field] = HashValue{Value: intIncrement}
			if err = params.SetValues(params.Context, map[string]interface{}{key: hash}); err != nil {
				return nil, err
			}
			return []byte(fmt.Sprintf(":%d\r\n", intIncrement)), nil
		}
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	if hash[field].Value == nil {
		hash[field] = HashValue{Value: 0}
	}

	switch hash[field].Value.(type) {
	default:
		return nil, fmt.Errorf("value at field %s is not a number", field)
	case int:
		i, _ := hash[field].Value.(int)
		if strings.EqualFold(params.Command[0], "hincrbyfloat") {
			hash[field] = HashValue{Value: float64(i) + floatIncrement}
		} else {
			hash[field] = HashValue{Value: i + intIncrement}
		}
	case float64:
		f, _ := hash[field].Value.(float64)
		if strings.EqualFold(params.Command[0], "hincrbyfloat") {
			hash[field] = HashValue{Value: f + floatIncrement}
		} else {
			hash[field] = HashValue{Value: f + float64(intIncrement)}
		}
	}

	if err = params.SetValues(params.Context, map[string]interface{}{key: hash}); err != nil {
		return nil, err
	}

	if f, ok := hash[field].Value.(float64); ok {
		return []byte(fmt.Sprintf("+%s\r\n", strconv.FormatFloat(f, 'f', -1, 64))), nil
	}

	i, _ := hash[field].Value.(int)
	return []byte(fmt.Sprintf(":%d\r\n", i)), nil
}

func handleHGETALL(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hgetallKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]

	if !keyExists {
		return []byte("*0\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	res := fmt.Sprintf("*%d\r\n", len(hash)*2)
	for field, value := range hash {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(field), field)
		if s, ok := value.Value.(string); ok {
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
		}

		if f, ok := value.Value.(float64); ok {
			fs := strconv.FormatFloat(f, 'f', -1, 64)
			res += fmt.Sprintf("$%d\r\n%s\r\n", len(fs), fs)
		}

		if d, ok := value.Value.(int); ok {
			res += fmt.Sprintf(":%d\r\n", d)
		}
	}

	return []byte(res), nil
}

func handleHEXISTS(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hexistsKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]
	field := params.Command[2]

	if !keyExists {
		return []byte(":0\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	if hash[field].Value != nil {
		return []byte(":1\r\n"), nil
	}

	return []byte(":0\r\n"), nil
}

func handleHDEL(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hdelKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	key := keys.WriteKeys[0]
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]
	fields := params.Command[2:]

	if !keyExists {
		return []byte(":0\r\n"), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	count := 0

	for _, field := range fields {
		if hash[field].Value != nil {
			delete(hash, field)
			count += 1
		}
	}

	if err = params.SetValues(params.Context, map[string]interface{}{key: hash}); err != nil {
		return nil, err
	}

	return []byte(fmt.Sprintf(":%d\r\n", count)), nil
}

func handleHEXPIRE(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hexpireKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}
	key := keys.WriteKeys[0]

	// HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field
	cmdargs := keys.WriteKeys[1:]
	seconds, err := strconv.ParseInt(cmdargs[0], 10, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("seconds must be integer, was provided %q", cmdargs[0]))
	}

	// FIELDS argument provides starting index to work off of to grab fields
	var fieldsIdx int
	if cmdargs[1] == "FIELDS" {
		fieldsIdx = 1
	} else if cmdargs[2] == "FIELDS" {
		fieldsIdx = 2
	} else {
		return nil, errors.New(fmt.Sprintf(constants.MissingArgResponse, "FIELDS"))
	}

	// index through numfields
	numfields, err := strconv.ParseInt(cmdargs[fieldsIdx+1], 10, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("numberfields must be integer, was provided %q", cmdargs[fieldsIdx+1]))
	}
	endIdx := fieldsIdx + 2 + int(numfields)
	fields := cmdargs[fieldsIdx+2 : endIdx]

	expireAt := params.GetClock().Now().Add(time.Duration(seconds) * time.Second)

	// build out response
	resp := "*" + fmt.Sprintf("%v", len(fields)) + "\r\n"

	// handle not hash or bad key
	keyExists := params.KeysExist(params.Context, keys.WriteKeys)[key]
	if !keyExists {
		for i := numfields; i > 0; i-- {
			resp = resp + ":-2\r\n"
		}
		return []byte(resp), nil
	}

	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value of key %s is not a hash", key)
	}

	// handle expire time of 0 seconds
	if seconds == 0 {
		for i := numfields; i > 0; i-- {
			resp = resp + ":2\r\n"
		}
		return []byte(resp), nil
	}

	if fieldsIdx == 2 {
		// Handle expire options
		switch strings.ToLower(cmdargs[1]) {
		case "nx":
			for _, f := range fields {
				_, ok := hash[f]
				if !ok {
					resp = resp + ":-2\r\n"
					continue
				}
				currentExpireAt := hash[f].ExpireAt
				if currentExpireAt != (time.Time{}) {
					resp = resp + ":0\r\n"
					continue
				}
				err = params.SetHashExpiry(params.Context, key, f, expireAt)
				if err != nil {
					return []byte(resp), err
				}

				resp = resp + ":1\r\n"

			}
		case "xx":
			for _, f := range fields {
				_, ok := hash[f]
				if !ok {
					resp = resp + ":-2\r\n"
					continue
				}
				currentExpireAt := hash[f].ExpireAt
				if currentExpireAt == (time.Time{}) {
					resp = resp + ":0\r\n"
					continue
				}
				err = params.SetHashExpiry(params.Context, key, f, expireAt)
				if err != nil {
					return []byte(resp), err
				}

				resp = resp + ":1\r\n"

			}
		case "gt":
			for _, f := range fields {
				_, ok := hash[f]
				if !ok {
					resp = resp + ":-2\r\n"
					continue
				}
				currentExpireAt := hash[f].ExpireAt
				//TODO
				if currentExpireAt == (time.Time{}) || expireAt.Before(currentExpireAt) {
					resp = resp + ":0\r\n"
					continue
				}
				err = params.SetHashExpiry(params.Context, key, f, expireAt)
				if err != nil {
					return []byte(resp), err
				}

				resp = resp + ":1\r\n"

			}
		case "lt":
			for _, f := range fields {
				_, ok := hash[f]
				if !ok {
					resp = resp + ":-2\r\n"
					continue
				}
				currentExpireAt := hash[f].ExpireAt
				if currentExpireAt != (time.Time{}) && currentExpireAt.Before(expireAt) {
					resp = resp + ":0\r\n"
					continue
				}
				err = params.SetHashExpiry(params.Context, key, f, expireAt)
				if err != nil {
					return []byte(resp), err
				}

				resp = resp + ":1\r\n"

			}
		default:
			return nil, fmt.Errorf("unknown option %s, must be one of 'NX', 'XX', 'GT', 'LT'.", strings.ToUpper(params.Command[3]))
		}
	} else {
		for _, f := range fields {
			_, ok := hash[f]
			if !ok {
				resp = resp + ":-2\r\n"
				continue
			}
			err = params.SetHashExpiry(params.Context, key, f, expireAt)
			if err != nil {
				return []byte(resp), err
			}

			resp = resp + ":1\r\n"

		}
	}

	// Array resp
	return []byte(resp), nil
}

func handleHTTL(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := httlKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	cmdargs := keys.ReadKeys[2:]
	numfields, err := strconv.ParseInt(cmdargs[0], 10, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("expire time must be integer, was provided %q", cmdargs[0]))
	}

	fields := cmdargs[1 : numfields+1]
	// init array response
	resp := "*" + fmt.Sprintf("%v", len(fields)) + "\r\n"

	// handle bad key
	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]
	if !keyExists {
		resp = resp + ":-2\r\n"
		return []byte(resp), nil
	}

	// handle not a hash
	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	// build out response
	for _, field := range fields {
		f, ok := hash[field]
		if !ok {
			resp = resp + ":-2\r\n"
			continue
		}
		if f.ExpireAt == (time.Time{}) {
			resp = resp + ":-1\r\n"
			continue
		}
		resp = resp + fmt.Sprintf(":%d\r\n", int(f.ExpireAt.Sub(params.GetClock().Now()).Round(time.Second).Seconds()))

	}

	// array response
	return []byte(resp), nil
}

func handleHPEXPIRETIME(params internal.HandlerFuncParams) ([]byte, error) {
	keys, err := hpexpiretimeKeyFunc(params.Command)
	if err != nil {
		return nil, err
	}

	cmdargs := keys.ReadKeys[2:]
	numfields, err := strconv.ParseInt(cmdargs[0], 10, 64)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("expire time must be integer, was provided %q", cmdargs[0]))
	}

	fields := cmdargs[1 : numfields+1]
	// init array response
	resp := "*" + fmt.Sprintf("%v", len(fields)) + "\r\n"

	// handle bad key
	key := keys.ReadKeys[0]
	keyExists := params.KeysExist(params.Context, keys.ReadKeys)[key]
	if !keyExists {
		return []byte("$-1\r\n"), nil
	}

	// handle not a hash
	hash, ok := params.GetValues(params.Context, []string{key})[key].(Hash)
	if !ok {
		return nil, fmt.Errorf("value at %s is not a hash", key)
	}

	for _, field := range fields {
		f, ok := hash[field]
		if !ok {
			// Field doesn't exist
			resp += ":-2\r\n"
			continue
		}

		if f.ExpireAt == (time.Time{}) {
			// No expiration set
			resp += "$-1\r\n"
			continue
		}
		// Calculate milliseconds until expiration
		millisUntilExpire := f.ExpireAt.Sub(params.GetClock().Now()).Milliseconds()
		resp += fmt.Sprintf(":%d\r\n", millisUntilExpire)
	}

	// build out response
	return []byte(resp), nil
}

func Commands() []internal.Command {
	return []internal.Command{
		{
			Command:    "hset",
			Module:     constants.HashModule,
			Categories: []string{constants.HashCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(HSET key field value [field value ...]) 
Set update each field of the hash with the corresponding value.`,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hsetKeyFunc,
			HandlerFunc:       handleHSET,
		},
		{
			Command:    "hsetnx",
			Module:     constants.HashModule,
			Categories: []string{constants.HashCategory, constants.WriteCategory, constants.FastCategory},
			Description: `(HSETNX key field value [field value ...]) 
Set hash field value only if the field does not exist.`,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hsetnxKeyFunc,
			HandlerFunc:       handleHSET,
		},
		{
			Command:    "hget",
			Module:     constants.HashModule,
			Categories: []string{constants.HashCategory, constants.ReadCategory, constants.FastCategory},
			Description: `(HGET key field [field ...]) 
Retrieve the value of each of the listed fields from the hash.`,
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hgetKeyFunc,
			HandlerFunc:       handleHGET,
		},
		{
			Command:    "hmget",
			Module:     constants.HashModule,
			Categories: []string{constants.HashCategory, constants.ReadCategory, constants.FastCategory},
			Description: `(HMGET key field [field ...]) 
Retrieve the value of each of the listed fields from the hash.`,
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hmgetKeyFunc,
			HandlerFunc:       handleHMGET,
		},
		{
			Command:    "hstrlen",
			Module:     constants.HashModule,
			Categories: []string{constants.HashCategory, constants.ReadCategory, constants.FastCategory},
			Description: `(HSTRLEN key field [field ...]) 
Return the string length of the values stored at the specified fields. 0 if the value does not exist.`,
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hstrlenKeyFunc,
			HandlerFunc:       handleHSTRLEN,
		},
		{
			Command:           "hvals",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.ReadCategory, constants.SlowCategory},
			Description:       `(HVALS key) Returns all the values of the hash at key.`,
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hvalsKeyFunc,
			HandlerFunc:       handleHVALS,
		},
		{
			Command:           "hrandfield",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.ReadCategory, constants.SlowCategory},
			Description:       `(HRANDFIELD key [count [WITHVALUES]]) Returns one or more random fields from the hash.`,
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hrandfieldKeyFunc,
			HandlerFunc:       handleHRANDFIELD,
		},
		{
			Command:           "hlen",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.ReadCategory, constants.FastCategory},
			Description:       `(HLEN key) Returns the number of fields in the hash.`,
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hlenKeyFunc,
			HandlerFunc:       handleHLEN,
		},
		{
			Command:           "hkeys",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.ReadCategory, constants.SlowCategory},
			Description:       `(HKEYS key) Returns all the fields in a hash.`,
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hkeysKeyFunc,
			HandlerFunc:       handleHKEYS,
		},
		{
			Command:           "hincrbyfloat",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.WriteCategory, constants.FastCategory},
			Description:       `(HINCRBYFLOAT key field increment) Increment the hash value by the float increment.`,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hincrbyKeyFunc,
			HandlerFunc:       handleHINCRBY,
		},
		{
			Command:           "hincrby",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.WriteCategory, constants.FastCategory},
			Description:       `(HINCRBY key field increment) Increment the hash value by the integer increment`,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hincrbyKeyFunc,
			HandlerFunc:       handleHINCRBY,
		},
		{
			Command:           "hgetall",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.ReadCategory, constants.SlowCategory},
			Description:       `(HGETALL key) Get all fields and values of a hash.`,
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hgetallKeyFunc,
			HandlerFunc:       handleHGETALL,
		},
		{
			Command:           "hexists",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.ReadCategory, constants.FastCategory},
			Description:       `(HEXISTS key field) Returns if field is an existing field in the hash.`,
			Sync:              false,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hexistsKeyFunc,
			HandlerFunc:       handleHEXISTS,
		},
		{
			Command:           "hdel",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.WriteCategory, constants.FastCategory},
			Description:       `(HDEL key field [field ...]) Deletes the specified fields from the hash.`,
			Sync:              true,
			Type:              "BUILT_IN",
			KeyExtractionFunc: hdelKeyFunc,
			HandlerFunc:       handleHDEL,
		},
		{
			Command:           "hexpire",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.WriteCategory, constants.FastCategory},
			Description:       `(HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]) Sets the expiration, in seconds, of a field in a hash.`,
			Sync:              true,
			KeyExtractionFunc: hexpireKeyFunc,
			HandlerFunc:       handleHEXPIRE,
		},
		{
			Command:           "httl",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.ReadCategory, constants.FastCategory},
			Description:       `HTTL key FIELDS numfields field [field ...] Returns the remaining TTL (time to live) of a hash key's field(s) that have a set expiration.`,
			Sync:              true,
			KeyExtractionFunc: httlKeyFunc,
			HandlerFunc:       handleHTTL,
		},
		{
			Command:           "hpexpireTime",
			Module:            constants.HashModule,
			Categories:        []string{constants.HashCategory, constants.ReadCategory, constants.FastCategory},
			Description:       `HPEXPIRETIME key field [field ...] Returns the absolute Unix timestamp in milliseconds since Unix epoch at which the given key's field(s) will expire. Returns -1 if field doesn't exist or has no expiry set.`,
			Sync:              true,
			KeyExtractionFunc: hpexpiretimeKeyFunc,
			HandlerFunc:       handleHPEXPIRETIME,
		},
	}
}
