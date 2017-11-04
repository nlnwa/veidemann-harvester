// Copyright © 2017 National Library of Norway
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

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
	"log"
	"os"
	api "veidemann_api"
	"veidemannctl/util"
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

		client, conn := util.NewControllerClient()
		defer conn.Close()

		for _, v := range result {
			switch v.(type) {
			case *api.CrawlEntity:
				r, err := client.SaveEntity(context.Background(), v.(*api.CrawlEntity))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *api.Seed:
				r, err := client.SaveSeed(context.Background(), v.(*api.Seed))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *api.CrawlJob:
				r, err := client.SaveCrawlJob(context.Background(), v.(*api.CrawlJob))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *api.CrawlConfig:
				r, err := client.SaveCrawlConfig(context.Background(), v.(*api.CrawlConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *api.CrawlScheduleConfig:
				r, err := client.SaveCrawlScheduleConfig(context.Background(), v.(*api.CrawlScheduleConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *api.BrowserConfig:
				r, err := client.SaveBrowserConfig(context.Background(), v.(*api.BrowserConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *api.PolitenessConfig:
				r, err := client.SavePolitenessConfig(context.Background(), v.(*api.PolitenessConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *api.BrowserScript:
				r, err := client.SaveBrowserScript(context.Background(), v.(*api.BrowserScript))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *api.CrawlHostGroupConfig:
				r, err := client.SaveCrawlHostGroupConfig(context.Background(), v.(*api.CrawlHostGroupConfig))
				handleError(v, err)
				fmt.Printf("Saved %T: %v\n", r, r.Meta.Name)
			case *api.LogLevels:
				r, err := client.SaveLogConfig(context.Background(), v.(*api.LogLevels))
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
