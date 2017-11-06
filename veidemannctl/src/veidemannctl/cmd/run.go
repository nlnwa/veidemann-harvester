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
	"log"

	"golang.org/x/net/context"
	api "veidemann_api"
	"veidemannctl/util"

	"fmt"
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
			client, conn := util.NewControllerClient()
			defer conn.Close()

			switch len(args) {
			case 1:
				// One argument (only jobId)
				request := api.RunCrawlRequest{JobId: args[0]}
				r, err := client.RunCrawl(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not run job: %v", err)
				}

				println(r.GetSeedExecutionId())
			case 2:
				// Two arguments (jobId and seedId)
				request := api.RunCrawlRequest{JobId: args[0], SeedId: args[1]}
				r, err := client.RunCrawl(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not run job: %v", err)
				}

				fmt.Println("Started executions: ")
				for _, eid := range r.GetSeedExecutionId() {
					fmt.Printf("  %s\n", eid)
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
