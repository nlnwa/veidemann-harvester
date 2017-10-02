package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
	"io"
	"log"
	"reflect"
)

func MarshalJson(w io.Writer, msg proto.Message) error {
	var values reflect.Value
	values = reflect.ValueOf(msg).Elem().FieldByName("Value")
	if values.IsValid() {
		if values.Len() == 0 {
			fmt.Println("Empty result")
			return nil
		}

		for i := 0; i < values.Len(); i++ {
			m := values.Index(i).Interface().(proto.Message)

			err := marshalElementJson(w, m)
			if err != nil {
				return err
			}
		}
	} else {
		m := reflect.ValueOf(msg).Interface().(proto.Message)

		err := marshalElementJson(w, m)
		if err != nil {
			return err
		}
	}

	return nil
}

func marshalElementJson(w io.Writer, msg proto.Message) error {
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

func EncodeJson(msg proto.Message) ([]byte, error) {
	var buf bytes.Buffer
	if err := jsonMarshaler.Marshal(&buf, msg); err != nil {
		log.Fatalf("Could not convert %v to YAML: %v", msg, err)
		return nil, err
	}

	values := make(map[string]interface{})
	json.Unmarshal(buf.Bytes(), &values)

	kind := GetObjectName(msg)

	values["kind"] = kind
	b, _ := json.Marshal(values)

	return b, nil
}