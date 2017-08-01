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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"os"
	"reflect"
	"strings"
)

var jsonMarshaler = jsonpb.Marshaler{EmitDefaults: true}
var jsonUnMarshaler = jsonpb.Unmarshaler{}

func Marshal(filename string, format string, msg proto.Message) error {
	var w io.Writer
	if filename == "" {
		w = os.Stdout
	} else {
		f, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Could not create file '%s': %v", filename, err)
			return err
		}
		defer f.Close()
		w = f
	}

	values := reflect.ValueOf(msg).Elem().FieldByName("Value")
	switch format {
	case "json":
		for i := 0; i < values.Len(); i++ {
			sliceVal := values.Index(i)
			m := sliceVal.Interface().(proto.Message)
			err := MarshalJson(w, m)
			if err != nil {
				return err
			}
		}
		return nil
	case "yaml":
		for i := 0; i < values.Len(); i++ {
			if i > 0 {
				w.Write([]byte("---\n"))
			}
			sliceVal := values.Index(i)
			m := sliceVal.Interface().(proto.Message)
			err := MarshalYaml(w, m)
			if err != nil {
				return err
			}
		}
		return nil
	default:
		log.Fatalf("Illegal format %s", format)
	}
	return nil
}

func MarshalJson(w io.Writer, msg proto.Message) error {
	r, err := EncodeJson(msg)
	if err != nil {
		log.Fatalf("Could not convert %v to JSON: %v", msg, err)
		return err
	}

	var out bytes.Buffer
	json.Indent(&out, r, "", "  ")

	fmt.Fprintln(w, out.String())
	return nil
}

func MarshalYaml(w io.Writer, msg proto.Message) error {
	r, err := EncodeJson(msg)
	if err != nil {
		log.Fatalf("Could not convert %v to JSON: %v", msg, err)
		return err
	}

	final, err := yaml.JSONToYAML(r)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return err
	}

	fmt.Fprintln(w, string(final))

	return nil
}

func EncodeJson(msg proto.Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := jsonMarshaler.Marshal(&buf, msg); err != nil {
		log.Fatalf("Could not convert %v to YAML: %v", msg, err)
		return nil, err
	}

	values := make(map[string]interface{})
	json.Unmarshal(buf.Bytes(), &values)

	kind := GetObjectName(reflect.TypeOf(msg))

	values["kind"] = kind
	b, _ := json.Marshal(values)

	return b, nil
}

func Unmarshal(filename string) ([]proto.Message, error) {
	result := make([]proto.Message, 0, 16)
	if filename == "" {
		return UnmarshalYaml(os.Stdin, result)
	} else {
		f, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Could not open file '%s': %v", filename, err)
			return nil, err
		}
		defer f.Close()
		if fi, _ := f.Stat(); fi.IsDir() {
			fis, _ := f.Readdir(0)
			for _, fi = range fis {
				if !fi.IsDir() && (strings.HasSuffix(fi.Name(), ".yaml") || strings.HasSuffix(fi.Name(), ".json")) {
					fmt.Println("Reading file: ", fi.Name())
					f, err = os.Open(fi.Name())
					if err != nil {
						log.Fatalf("Could not open file '%s': %v", filename, err)
						return nil, err
					}
					defer f.Close()

					if strings.HasSuffix(f.Name(), ".yaml") {
						result, err = UnmarshalYaml(f, result)
					} else {
						result, err = UnmarshalJson(f, result)
					}
					if err != nil {
						return nil, err
					}
				}
			}
			return result, nil
		} else {
			if strings.HasSuffix(f.Name(), ".yaml") {
				return UnmarshalYaml(f, result)
			} else {
				return UnmarshalJson(f, result)
			}
		}
	}
	return result, nil
}

func ReadYamlDocument(r *bufio.Reader) ([]byte, error) {
	delim := []byte{'-', '-', '-'}
	var (
		inDoc  bool  = true
		err    error = nil
		l, doc []byte
	)
	for inDoc && err == nil {
		isPrefix := true
		ln := []byte{}
		for isPrefix && err == nil {
			l, isPrefix, err = r.ReadLine()
			ln = append(ln, l...)
		}

		if len(ln) >= 3 && bytes.Equal(delim, ln[:3]) {
			inDoc = false
		} else {
			doc = append(doc, ln...)
			doc = append(doc, '\n')
		}
	}
	return doc, err
}

func UnmarshalYaml(r io.Reader, result []proto.Message) ([]proto.Message, error) {
	br := bufio.NewReader(r)

	var (
		data    []byte
		readErr error = nil
	)
	for readErr == nil {
		data, readErr = ReadYamlDocument(br)
		if readErr != nil && readErr != io.EOF {
			return nil, readErr
		}

		var val interface{}
		err := yaml.Unmarshal(data, &val)
		if err != nil {
			log.Fatal(err)
		}

		v := val.(map[string]interface{})
		k := v["kind"]
		if k == nil {
			return nil, fmt.Errorf("Missing kind")
		}
		kind := k.(string)
		delete(v, "kind")

		b, _ := json.Marshal(&v)

		buf := bytes.NewBuffer(b)
		t := GetObjectType(kind)
		if t == nil {
			return nil, fmt.Errorf("Unknown kind '%v'", kind)
		}

		target := reflect.New(t.Elem()).Interface().(proto.Message)

		jsonUnMarshaler.Unmarshal(buf, target)
		result = append(result, target)
	}
	return result, nil
}

func UnmarshalJson(r io.Reader, result []proto.Message) ([]proto.Message, error) {
	dec := json.NewDecoder(r)
	for dec.More() {
		var val interface{}
		err := dec.Decode(&val)
		if err != nil {
			log.Fatal(err)
		}
		v := val.(map[string]interface{})
		kind := v["kind"].(string)
		delete(v, "kind")

		b, _ := json.Marshal(&v)

		buf := bytes.NewBuffer(b)
		t := GetObjectType(kind)

		target := reflect.New(t.Elem()).Interface().(proto.Message)

		jsonUnMarshaler.Unmarshal(buf, target)
		result = append(result, target)
	}

	return result, nil
}
