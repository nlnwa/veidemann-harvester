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
	api "veidemann_api"

	"context"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/spf13/cobra"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"text/template"
	"veidemannctl/bindata"
	"veidemannctl/util"
)

var (
	executionId string
	uri         string
	img         bool
	pageSize    int32
	page        int32
	goTemplate  string
	filter      []string
)

// reportCmd represents the report command
var reportCmd = &cobra.Command{
	Use:   "report",
	Short: "Get log report",
	Long: `Request a report.

` + printValidReportTypes(),

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) > 0 {
			client, conn := util.NewReportClient()
			defer conn.Close()

			switch args[0] {
			case "crawllog":
				request := api.CrawlLogListRequest{}
				if len(args) > 1 {
					request.WarcId = args[1:]
				}
				if executionId != "" {
					request.ExecutionId = executionId
				}
				request.Filter = applyFilter(filter)
				request.Page = page
				request.PageSize = pageSize

				r, err := client.ListCrawlLogs(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get crawl log: %v", err)
				}

				applyTemplate(r, "crawllog.template")
			case "pagelog":
				request := api.PageLogListRequest{}
				if len(args) > 1 {
					request.WarcId = args[1:]
				}
				if executionId != "" {
					request.ExecutionId = executionId
				}
				request.Filter = applyFilter(filter)
				request.Page = page
				request.PageSize = pageSize

				r, err := client.ListPageLogs(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get page log: %v", err)
				}

				applyTemplate(r, "pagelog.template")
			case "screenshot":
				request := api.ScreenshotListRequest{}
				if len(args) > 1 {
					request.Id = args[1:]
				}
				if executionId != "" {
					request.ExecutionId = executionId
				}
				if img {
					request.ImgData = true
				}
				request.Filter = applyFilter(filter)
				request.Page = page
				request.PageSize = pageSize

				r, err := client.ListScreenshots(context.Background(), &request)
				if err != nil {
					log.Fatalf("could not get page log: %v", err)
				}

				if img {
					printScreenshot(r.Value[0])
				} else {
					applyTemplate(r, "screenshot.template")
				}
			default:
				fmt.Printf("Unknown report type\n")
				cmd.Usage()
			}
		} else {
			fmt.Print("You must specify the report type to get. ")
			fmt.Println(printValidReportTypes())
			fmt.Println("See 'veidemannctl get -h' for help")
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
	reportCmd.PersistentFlags().StringVarP(&executionId, "executionid", "e", "", "All objects by Execution ID")
	reportCmd.PersistentFlags().StringVarP(&uri, "uri", "", "", "All screenshots by URI")
	reportCmd.PersistentFlags().BoolVarP(&img, "img", "", false, "Image binary")
	reportCmd.PersistentFlags().Int32VarP(&pageSize, "pagesize", "s", 10, "Number of objects to get")
	reportCmd.PersistentFlags().Int32VarP(&page, "page", "p", 0, "The page number")
	reportCmd.PersistentFlags().StringVarP(&goTemplate, "template", "t", "", "A Go template used to format the output")
	reportCmd.PersistentFlags().StringSliceVarP(&filter, "filter", "f", nil, "Filters")
}

func applyFilter(filter []string) []*api.Filter {
	var result []*api.Filter
	for _, f := range filter {
		tokens := strings.SplitN(f, " ", 3)
		op := api.Filter_Operator(api.Filter_Operator_value[strings.ToUpper(tokens[1])])
		result = append(result, &api.Filter{tokens[0], op, tokens[2]})
	}
	return result
}

func printValidReportTypes() string {
	reportNames := []string{"crawllog", "pagelog", "screenshot"}
	var names string
	for _, v := range reportNames {
		names += fmt.Sprintf("  * %s\n", v)
	}
	return fmt.Sprintf("Valid report types include:\n%s\n", names)
}

func applyTemplate(msg proto.Message, defaultTemplate string) {
	var data []byte
	var err error
	if goTemplate == "" {
		data, err = bindata.Asset(defaultTemplate)
		if err != nil {
			panic(err)
		}
	} else {
		data, err = ioutil.ReadFile(goTemplate)
		if err != nil {
			panic(err)
		}
	}

	ESC := string(0x1b)
	funcMap := template.FuncMap{
		"reset":         func() string { return ESC + "[0m" },
		"bold":          func() string { return ESC + "[1m" },
		"inverse":       func() string { return ESC + "[7m" },
		"red":           func() string { return ESC + "[31m" },
		"green":         func() string { return ESC + "[32m" },
		"yellow":        func() string { return ESC + "[33m" },
		"blue":          func() string { return ESC + "[34m" },
		"magenta":       func() string { return ESC + "[35m" },
		"cyan":          func() string { return ESC + "[36m" },
		"brightred":     func() string { return ESC + "[1;31m" },
		"brightgreen":   func() string { return ESC + "[1;32m" },
		"brightyellow":  func() string { return ESC + "[1;33m" },
		"brightblue":    func() string { return ESC + "[1;34m" },
		"brightmagenta": func() string { return ESC + "[1;35m" },
		"brightcyan":    func() string { return ESC + "[1;36m" },
		"bgwhite":       func() string { return ESC + "[47m" },
		"bgbrightblack": func() string { return ESC + "[100m" },
		"time":          func(ts *tspb.Timestamp) string { return ptypes.TimestampString(ts) },
	}

	tmpl, err := template.New(defaultTemplate).Funcs(funcMap).Parse(string(data))
	if err != nil {
		panic(err)
	}
	err = tmpl.Execute(os.Stdout, msg)
	if err != nil {
		panic(err)
	}
}

func printPageLogLine(pageLog *api.PageLog) {
	fmt.Printf("---\n%s %s %s\nResources:\n", pageLog.ExecutionId, pageLog.Uri, pageLog.WarcId)
	for _, r := range pageLog.Resource {
		fmt.Printf(" - %s %v %s %v %v %s %s %s\n", r.Uri, r.StatusCode, r.DiscoveryPath, r.FromCache, r.Renderable, r.MimeType, r.ResourceType, r.WarcId)
	}
	fmt.Println("Outlinks:")
	for _, r := range pageLog.Outlink {
		fmt.Printf(" - %s\n", r)
	}
}

func printScreenshot(screenshot *api.Screenshot) {
	os.Stdout.Write(screenshot.Img)
}
