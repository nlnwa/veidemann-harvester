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

package main

import (
	"broproxctl/cmd"
	log "github.com/sirupsen/logrus"
)

//go:generate go-bindata -prefix "../../res/" -pkg bindata -o bindata/templates.go ../../res/...
func main() {
	cmd.Execute()
}

func init() {
	log.SetLevel(log.WarnLevel)
}
