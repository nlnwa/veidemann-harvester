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

package cmd

import (
	"fmt"
	"os"

	"broproxctl/util"
	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"net/url"
)

var (
	cfgFile           string
	controllerAddress string
	Idp               string
	debug             bool
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "broproxctl",
	Short: "Broprox command line client",
	Long:  `A command line client for Broprox which can manipulate configs and request status of the crawler.`,

	// Uncomment the following line if your bare application
	// has an action associated with it:
	//Run: func(cmd *cobra.Command, args []string) {
	//},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.broproxctl.yaml)")

	RootCmd.PersistentFlags().StringVarP(&controllerAddress, "controllerAddress", "c", "localhost:50051", "Address to the Controller service")
	viper.BindPFlag("controllerAddress", RootCmd.PersistentFlags().Lookup("controllerAddress"))

	RootCmd.PersistentFlags().StringVarP(&Idp, "idp", "", "", "Address to identity provider")
	viper.BindPFlag("idp", RootCmd.PersistentFlags().Lookup("idp"))

	RootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "Turn on debugging")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if debug {
		log.SetLevel(log.DebugLevel)
	}

	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".broproxctl" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".broproxctl")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Debugf("Using config file: %s", viper.ConfigFileUsed())
	}

	if viper.GetString("idp") == "" {
		u, err := url.Parse(viper.GetString("controllerAddress"))
		if err != nil {
			log.Fatal(err)
			// handle error
		}
		u.Host = u.Hostname() + ":32000"
		u.Path = "/dex"
		Idp = u.String()
	} else {
		Idp = viper.GetString("idp")
	}

	if RootCmd.PersistentFlags().Changed("controllerAddress") || RootCmd.PersistentFlags().Changed("idp") {
		util.WriteConfig()
	}
}
