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
	"bytes"
	"fmt"
	"github.com/ghodss/yaml"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"log"
	"os"
)

var jsonMarshaler = jsonpb.Marshaler{Indent: "  ", EmitDefaults: true}

func Marshal(filename string, format string, msg proto.Message) error {
	switch format {
	case "json":
		return MarshalJson(filename, msg)
	case "yaml":
		return MarshalYaml(filename, msg)
	default:
		log.Fatalf("Illegal format %s", format)
	}
	return nil
}

func MarshalJson(filename string, msg proto.Message) error {
	if filename == "" {
		final, err := jsonMarshaler.MarshalToString(msg)
		if err != nil {
			log.Fatalf("Could not convert %v to JSON: %v", msg, err)
			return err
		}

		fmt.Println(final)
	} else {
		f, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Could not create file '%s': %v", filename, err)
			return err
		}
		defer f.Close()

		if err := jsonMarshaler.Marshal(f, msg); err != nil {
			log.Fatalf("Could not convert %v to JSON: %v", msg, err)
			return err
		}
		return nil
	}

	return nil
}

func MarshalYaml(filename string, msg proto.Message) error {
	var buf bytes.Buffer
	if err := jsonMarshaler.Marshal(&buf, msg); err != nil {
		log.Fatalf("Could not convert %v to YAML: %v", msg, err)
		return err
	}

	final, err := yaml.JSONToYAML(buf.Bytes())
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return err
	}

	if filename == "" {
		fmt.Println(string(final))
	} else {
		f, err := os.Create(filename)
		if err != nil {
			log.Fatalf("Could not create file '%s': %v", filename, err)
			return err
		}
		defer f.Close()

		f.Write(final)
		return nil
	}

	return nil
}
