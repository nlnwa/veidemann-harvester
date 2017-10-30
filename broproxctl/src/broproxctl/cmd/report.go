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
	"fmt"

	"broproxctl/util"
	"context"
	"github.com/spf13/cobra"
	"log"
	"os"
)

var (
	warcId      string
	executionId string
	id          string
	uri         string
	img         bool
	pageSize    int32
	page        int32
)

// reportCmd represents the report command
var reportCmd = &cobra.Command{
	Use:   "report",
	Short: "Get log report",
	Long: `Request a report.

` + printValidReportTypes(),

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 1 {
			client, conn := util.NewReportClient()
			defer conn.Close()

			switch args[0] {
			case "crawllog":
				request := bp.CrawlLogListRequest{}
				if warcId != "" {
					request.Qry = &bp.CrawlLogListRequest_WarcId{warcId}
				}
				if executionId != "" {
					request.Qry = &bp.CrawlLogListRequest_ExecutionId{executionId}
				}
				request.Page = page
				request.PageSize = pageSize

				r, err := client.ListCrawlLogs(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get crawl log: %v", err)
				}

				for _, crawlLog := range r.Value {
					printCrawlLogLine(crawlLog)
				}
			case "pagelog":
				request := bp.PageLogListRequest{}
				if warcId != "" {
					request.Qry = &bp.PageLogListRequest_WarcId{warcId}
				}
				if executionId != "" {
					request.Qry = &bp.PageLogListRequest_ExecutionId{executionId}
				}
				request.Page = page
				request.PageSize = pageSize

				r, err := client.ListPageLogs(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get page log: %v", err)
				}

				for _, pageLog := range r.Value {
					printPageLogLine(pageLog)
				}
			case "screenshot":
				request := bp.ScreenshotListRequest{}
				if id != "" {
					request.Qry = &bp.ScreenshotListRequest_Id{id}
				}
				if executionId != "" {
					request.Qry = &bp.ScreenshotListRequest_ExecutionId{executionId}
				}
				if uri != "" {
					request.Qry = &bp.ScreenshotListRequest_Uri{uri}
				}
				request.Page = page
				request.PageSize = pageSize

				r, err := client.ListScreenshots(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get page log: %v", err)
				}

				if img {
					printScreenshot(r.Value[0])
				} else {
					for _, screenshot := range r.Value {
						printScreenshotLine(screenshot)
					}
				}
			default:
				fmt.Printf("Unknown report type\n")
				cmd.Usage()
			}
		} else {
			fmt.Print("You must specify the report type to get. ")
			fmt.Println(printValidReportTypes())
			fmt.Println("See 'broproxctl get -h' for help")
		}
	},
}

func init() {
	RootCmd.AddCommand(reportCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// reportCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// reportCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	reportCmd.Flags().StringVarP(&warcId, "warcid", "w", "", "Single object by Warc ID")
	reportCmd.PersistentFlags().StringVarP(&executionId, "executionid", "e", "", "All objects by Execution ID")
	reportCmd.PersistentFlags().StringVarP(&id, "id", "", "", "Single screenshot by ID")
	reportCmd.PersistentFlags().StringVarP(&uri, "uri", "", "", "All screenshots by URI")
	reportCmd.PersistentFlags().BoolVarP(&img, "img", "", false, "Image binary")
	reportCmd.PersistentFlags().Int32VarP(&pageSize, "pagesize", "s", 5, "Number of objects to get")
	reportCmd.PersistentFlags().Int32VarP(&page, "page", "p", 0, "The page number")
}

func printValidReportTypes() string {
	reportNames := []string{"crawllog", "pagelog", "screenshot"}
	var names string
	for _, v := range reportNames {
		names += fmt.Sprintf("  * %s\n", v)
	}
	return fmt.Sprintf("Valid report types include:\n%s\n", names)
}

func printCrawlLogLine(crawlLog *bp.CrawlLog) {
	fmt.Printf("---\n%s\n", crawlLog)
}

func printPageLogLine(pageLog *bp.PageLog) {
	fmt.Printf("---\n%s %s %s\nResources:\n", pageLog.ExecutionId, pageLog.Uri, pageLog.WarcId)
	for _, r := range pageLog.Resource {
		fmt.Printf(" - %s %v %s %v %v %s %s %s\n", r.Uri, r.StatusCode, r.DiscoveryPath, r.FromCache, r.Renderable, r.MimeType, r.ResourceType, r.WarcId)
	}
	fmt.Println("Outlinks:")
	for _, r := range pageLog.Outlink {
		fmt.Printf(" - %s\n", r)
	}
}

func printScreenshotLine(screenshot *bp.Screenshot) {
	fmt.Printf("---\n%s %s %s\n", screenshot.Id, screenshot.ExecutionId, screenshot.Uri)
}

func printScreenshot(screenshot *bp.Screenshot) {
	os.Stdout.Write(screenshot.Img)
}
