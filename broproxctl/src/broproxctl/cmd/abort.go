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
	bp "broprox"

	"broproxctl/util"
	"context"
	"github.com/spf13/cobra"
	"log"
)

// abortCmd represents the abort command
var abortCmd = &cobra.Command{
	Use:   "abort",
	Short: "Abort one or more crawl executions",
	Long:  `Abort one or more crawl executions.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			idToken := util.GetRawIdToken(Idp)
			client, conn := util.NewControllerClient(idToken)
			defer conn.Close()

			for _, arg := range args {
				request := bp.AbortCrawlRequest{ExecutionId: arg}
				_, err := client.AbortCrawl(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not abort execution '%v': %v", arg, err)
				}
			}
		} else {
			cmd.Usage()
		}
	},
}

func init() {
	RootCmd.AddCommand(abortCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// abortCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// abortCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
