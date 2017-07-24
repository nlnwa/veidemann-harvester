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
	"log"
	"os"

	bp "broprox"
	"broproxctl/util"
	"golang.org/x/net/context"

	"github.com/spf13/cobra"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run jobId [seedId]",
	Short: "Immediately run a crawl",
	Long: `Run a crawl. If seedId is submitted only this seed will be run using the configuration
from the submitted jobId. This will run even if the seed is not configured to use the jobId.
If seedId is not submitted then all the seeds wich are configured to use the submitted jobId will be crawled.`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			client := util.Connect()
			switch len(args) {
			case 1:
				// One argument (only jobId)
				request := bp.RunCrawlRequest{JobId: args[0]}
				r, err := client.RunCrawl(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get entity: %v", err)
				}

				if util.MarshalJson(file, r) != nil {
					os.Exit(1)
				}
			case 2:
				// Two arguments (jobId and seedId)
				request := bp.RunCrawlRequest{JobId: args[0], SeedId: args[1]}
				r, err := client.RunCrawl(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get entity: %v", err)
				}

				if util.MarshalJson(file, r) != nil {
					os.Exit(1)
				}
			}
		} else {
			cmd.Usage()
		}
	},
}

func init() {
	RootCmd.AddCommand(runCmd)
}
