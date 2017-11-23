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
	"github.com/ghodss/yaml"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
)

type config struct {
	ControllerAddress string `json:"controllerAddress"`
	Idp               string `json:"idp"`
	AccessToken       string `json:"accessToken"`
	Nonce             string `json:"nonce"`
	RootCAs           string `json:"rootCAs"`
}

func WriteConfig() {
	log.Debug("Writing config")

	c := config{
		viper.GetString("controllerAddress"),
		viper.GetString("idp"),
		viper.GetString("accessToken"),
		viper.GetString("nonce"),
		viper.GetString("rootCAs"),
	}

	y, err := yaml.Marshal(c)
	if err != nil {
		log.Fatalf("err: %v\n", err)
	}

	f, err := os.Create(viper.ConfigFileUsed())
	if err != nil {
		log.Fatalf("Could not create file '%s': %v", viper.ConfigFileUsed(), err)
	}
	f.Chmod(0600)
	defer f.Close()

	f.Write(y)
}
