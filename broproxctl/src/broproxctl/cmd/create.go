// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
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

	bp "broprox"
	"broproxctl/util"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"log"
	"os"
)

var filename string

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "Create or update a config object",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {

		if filename == "" {
			cmd.Usage()
			os.Exit(1)
		} else if filename == "-" {
			filename = ""
		}
		result, err := util.Unmarshal(filename)
		if err != nil {
			log.Fatalf("Parse error: %v", err)
			os.Exit(1)
		}

		idToken := util.GetRawIdToken(Idp)
		client, conn := util.NewControllerClient(idToken)
		defer conn.Close()

		for _, v := range result {
			switch v.(type) {
			case *bp.CrawlEntity:
				r, err := client.SaveEntity(context.Background(), v.(*bp.CrawlEntity))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *bp.Seed:
				r, err := client.SaveSeed(context.Background(), v.(*bp.Seed))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *bp.CrawlJob:
				r, err := client.SaveCrawlJob(context.Background(), v.(*bp.CrawlJob))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *bp.CrawlConfig:
				r, err := client.SaveCrawlConfig(context.Background(), v.(*bp.CrawlConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *bp.CrawlScheduleConfig:
				r, err := client.SaveCrawlScheduleConfig(context.Background(), v.(*bp.CrawlScheduleConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *bp.BrowserConfig:
				r, err := client.SaveBrowserConfig(context.Background(), v.(*bp.BrowserConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *bp.PolitenessConfig:
				r, err := client.SavePolitenessConfig(context.Background(), v.(*bp.PolitenessConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *bp.BrowserScript:
				r, err := client.SaveBrowserScript(context.Background(), v.(*bp.BrowserScript))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *bp.CrawlHostGroupConfig:
				r, err := client.SaveCrawlHostGroupConfig(context.Background(), v.(*bp.CrawlHostGroupConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *bp.LogLevels:
				r, err := client.SaveLogConfig(context.Background(), v.(*bp.LogLevels))
				handleError(v, err)
				fmt.Printf("Saved %T: Loglevels\n", r)
			}
		}
	},
}

func handleError(msg proto.Message, err error) {
	if err != nil {
		log.Fatalf("Could not save %T: %v", msg, err)
		os.Exit(2)
	}
}

func init() {
	RootCmd.AddCommand(createCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// createCmd.PersistentFlags().String("foo", "", "A help for foo")
	createCmd.PersistentFlags().StringVarP(&filename, "input", "i", "", "File name to read from. "+
		"If input is a directory, all files ending in .yaml or .json will be tried. An input of '-' will read from stdin.")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// createCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
