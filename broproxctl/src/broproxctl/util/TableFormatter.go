// Copyright © 2017 National Library of Norway.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"fmt"
	tm "github.com/buger/goterm"
	"github.com/golang/protobuf/proto"
	"io"
	"reflect"
	"strings"
)

var debugEnabled = false

func MarshalTable(w io.Writer, msg proto.Message) error {
	table := tm.NewTable(2, 10, 2, ' ', 0)

	var values reflect.Value
	values = reflect.ValueOf(msg).Elem().FieldByName("Value")
	if values.IsValid() {
		m := values.Index(0).Interface().(proto.Message)
		tableDef := GetTableDef(m)
		formatHeader(table, tableDef, m)

		for i := 0; i < values.Len(); i++ {
			err := formatData(table, tableDef, m)
			if err != nil {
				return err
			}
			debug(m)
		}
	} else {
		m := reflect.ValueOf(msg).Interface().(proto.Message)

		debug(m)

		tableDef := GetTableDef(m)
		formatHeader(table, tableDef, m)

		err := formatData(table, tableDef, m)
		if err != nil {
			return err
		}
	}

	tm.Println(table)
	tm.Flush()

	return nil
}

func debug(m proto.Message) {
	if debugEnabled {
		v := reflect.ValueOf(m).Elem()
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			fmt.Printf("%d: %s %s = %v\n", i,
				t.Field(i).Name, f.Type(), f.Interface())
		}
	}
}

func formatHeader(t *tm.Table, tableDef []string, msg proto.Message) error {
	var header string
	for idx, tab := range tableDef {
		if idx > 0 {
			header += "\t"
		}
		header += tab
	}

	fmt.Fprintf(t, "\n%s\n", header)
	return nil
}

func formatData(t *tm.Table, tableDef []string, msg proto.Message) error {
	var line string
	for idx, tab := range tableDef {
		if idx > 0 {
			line += "\t"
		}
		line += fmt.Sprint(getField(msg, tab))
	}

	fmt.Fprintf(t, "%s\n", line)
	return nil
}

func getField(msg proto.Message, fieldName string) reflect.Value {
	tokens := strings.Split(fieldName, ".")
	v := reflect.ValueOf(msg)
	for _, tok := range tokens {
		v = reflect.Indirect(v)
		if v.Kind() == reflect.Interface {
			v = reflect.Indirect(reflect.ValueOf(v.Interface()))
		}
		v = v.FieldByName(tok)
	}
	return v
}
