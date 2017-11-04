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

	"github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"strings"
	"veidemannctl/util"
)

var (
	cfgFile           string
	controllerAddress string
	Idp               string
	debug             bool
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "veidemannctl",
	Short: "Veidemann command line client",
	Long:  `A command line client for Veidemann which can manipulate configs and request status of the crawler.`,

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

	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.veidemannctl.yaml)")

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

	var home string
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		var err error
		home, err = homedir.Dir()
		if err != nil {
			log.Fatal(err)
		}

		// Search config in home directory with name ".veidemannctl" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".veidemannctl")
	}

	viper.AutomaticEnv() // read in environment variables that match
	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Debugf("Using config file: %s", viper.ConfigFileUsed())
	} else {
		viper.SetConfigFile(home + "/.veidemannctl.yaml")
	}

	if viper.GetString("idp") == "" && viper.GetString("controllerAddress") != "" {
		ca := strings.SplitN(viper.GetString("controllerAddress"), ":", 2)
		host := ca[0]
		Idp = "https://" + host + ":3200/dex"
	} else {
		Idp = viper.GetString("idp")
	}

	if RootCmd.PersistentFlags().Changed("controllerAddress") || RootCmd.PersistentFlags().Changed("idp") {
		util.WriteConfig()
	}
}
