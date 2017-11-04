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
	"github.com/golang/protobuf/proto"
	"reflect"
	"strings"
	api "veidemann_api"
)

var objectTypes = []struct {
	vType  reflect.Type
	vName  string
	tabDef []string
}{
	{reflect.TypeOf(&api.CrawlEntity{}), "entity", []string{"Id", "Meta.Name", "Meta.Description"}},
	{reflect.TypeOf(&api.Seed{}), "seed", []string{"Id", "Meta.Name", "EntityId", "Scope.SurtPrefix", "JobId", "Disabled"}},
	{reflect.TypeOf(&api.CrawlJob{}), "job", []string{"Id", "Meta.Name", "Meta.Description", "ScheduleConfigOrId.ScheduleId", "Limits", "CrawlConfigOrId.CrawlConfigId", "Disabled"}},
	{reflect.TypeOf(&api.CrawlConfig{}), "crawlconfig", []string{"Id", "Meta.Name", "Meta.Description", "BrowserConfigOrId.BrowserConfigId", "PolitenessOrId.PolitenessId", "Extra"}},
	{reflect.TypeOf(&api.CrawlScheduleConfig{}), "schedule", []string{"Id", "Meta.Name", "Meta.Description", "CronExpression", "ValidFrom", "ValidTo"}},
	{reflect.TypeOf(&api.BrowserConfig{}), "browser", []string{"Id", "Meta.Name", "Meta.Description", "UserAgent", "WindowWidth", "WindowHeight", "PageLoadTimeoutMs", "SleepAfterPageloadMs"}},
	{reflect.TypeOf(&api.PolitenessConfig{}), "politeness", []string{"Id", "Meta.Name", "Meta.Description", "RobotsPolicy", "MinTimeBetweenPageLoadMs", "MaxTimeBetweenPageLoadMs", "DelayFactor", "MaxRetries", "RetryDelaySeconds", "CrawlHostGroupSelector"}},
	{reflect.TypeOf(&api.BrowserScript{}), "script", []string{"Id", "Meta.Name", "Meta.Description", "Script", "UrlRegexp"}},
	{reflect.TypeOf(&api.CrawlHostGroupConfig{}), "group", []string{"Id", "Meta.Name", "Meta.Description", "IpRange"}},
	{reflect.TypeOf(&api.LogLevels{}), "loglevel", []string{"LogLevel"}},
	{reflect.TypeOf(&api.RoleMapping{}), "role", []string{"Id", "EmailOrGroup.Email", "EmailOrGroup.Group", "Role"}},
	{reflect.TypeOf(&api.RoleList{}), "activerole", []string{"Role"}},
}

// Get mapping from 'kind' to 'type'
func GetObjectType(Name string) reflect.Type {
	Name = strings.ToLower(Name)
	for _, ot := range objectTypes {
		if ot.vName == Name {
			return ot.vType
		}
	}
	return nil
}

// Get mapping from 'type' to 'kind'
func GetObjectName(msg proto.Message) string {
	t := reflect.TypeOf(msg)
	for _, ot := range objectTypes {
		if ot.vType == t {
			return ot.vName
		}
	}
	return ""
}

// Get definitions for columns in table format
func GetTableDef(msg proto.Message) []string {
	t := reflect.TypeOf(msg)
	for _, ot := range objectTypes {
		if ot.vType == t {
			return ot.tabDef
		}
	}
	return nil
}

func GetObjectNames() []string {
	result := make([]string, len(objectTypes))
	for idx, ot := range objectTypes {
		result[idx] = ot.vName
	}
	return result
}
