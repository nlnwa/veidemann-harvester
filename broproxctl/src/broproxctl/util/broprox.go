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
	bp "broprox"
	"strings"
)

func CreateSelector(labelString string) *bp.Selector {
	ls := strings.Split(labelString, ",")
	labels := make([]*bp.Label, len(ls))
	for i := range ls {
		l := strings.Split(ls[i], "=")
		switch len(l) {
		case 2:
			labels[i] = &bp.Label{Key: strings.TrimSpace(l[0]), Value: strings.TrimSpace(l[1])}
		case 1:
			labels[i] = &bp.Label{Value: strings.TrimSpace(l[0])}
		}
	}

	return &bp.Selector{labels}
}
