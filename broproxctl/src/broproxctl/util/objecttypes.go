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
	"broprox"
	"reflect"
	"strings"
)

var objectTypes = []struct {
	valueType reflect.Type
	valueName string
}{
	{reflect.TypeOf(&broprox.Seed{}), "seed"},
	{reflect.TypeOf(&broprox.CrawlEntity{}), "entity"},
}

func GetObjectType(Name string) reflect.Type {
	Name = strings.ToLower(Name)
	for _, ot := range objectTypes {
		if ot.valueName == Name {
			return ot.valueType
		}
	}
	return nil
}

func GetObjectName(Type reflect.Type) string {
	for _, ot := range objectTypes {
		if ot.valueType == Type {
			return ot.valueName
		}
	}
	return ""
}
