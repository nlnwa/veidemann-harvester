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
	"log"
	"os"

	"github.com/spf13/cobra"

	bp "broprox"
	"broproxctl/util"
	"github.com/golang/protobuf/ptypes/empty"
	//"github.com/golang/protobuf/ptypes/timestamp"
	"golang.org/x/net/context"
)

var (
	label  string
	file   string
	format string
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get [object_type]",
	Short: "Get the value(s) for an object type",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 1 {
			client := util.Connect()
			var selector *bp.Selector

			if label != "" {
				selector = util.CreateSelector(label)
			}

			switch args[0] {
			case "entity":
				request := bp.ListRequest{}
				if selector != nil {
					request.Qry = &bp.ListRequest_Selector{selector}
				}
				r, err := client.ListCrawlEntities(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get entity: %v", err)
				}

				if util.Marshal(file, format, r) != nil {
					os.Exit(1)
				}
			case "seed":
				request := bp.SeedListRequest{}
				if selector != nil {
					request.Qry = &bp.SeedListRequest_Selector{selector}
				}
				r, err := client.ListSeeds(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get seed: %v", err)
				}

				if util.Marshal(file, format, r) != nil {
					os.Exit(1)
				}
			case "job":
				request := bp.CrawlJobListRequest{}
				if selector != nil {
					request.Qry = &bp.CrawlJobListRequest_Selector{selector}
				}
				r, err := client.ListCrawlJobs(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get job: %v", err)
				}

				if util.Marshal(file, format, r) != nil {
					os.Exit(1)
				}
			case "loglevel":
				r, err := client.GetLogConfig(context.Background(), &empty.Empty{})
				if err != nil {
					log.Fatalf("could not get log config: %v", err)
				}

				if util.Marshal(file, format, r) != nil {
					os.Exit(1)
				}
			default:
				fmt.Printf("Unknown object type\n")
				cmd.Usage()
			}
		} else {
			cmd.Usage()
		}
	},
}

func init() {
	RootCmd.AddCommand(getCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	getCmd.PersistentFlags().StringVarP(&label, "label", "l", "", "List objects by label")
	getCmd.PersistentFlags().StringVarP(&format, "format", "f", "yaml", "Output format (json|yaml)")
	getCmd.PersistentFlags().StringVarP(&file, "output", "o", "", "File name to write to")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// getCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
