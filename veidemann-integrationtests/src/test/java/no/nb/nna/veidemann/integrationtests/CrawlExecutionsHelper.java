/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.veidemann.integrationtests;

import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.report.v1.CrawlExecutionsListRequest;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;

import java.util.List;
import java.util.stream.Collectors;

public class CrawlExecutionsHelper {
    final List<CrawlExecutionStatus> executionsListReply;

    public CrawlExecutionsHelper(String jobExecutionId) throws DbException {
        CrawlExecutionsListRequest.Builder celr = CrawlExecutionsListRequest.newBuilder().setPageSize(500);
        celr.getQueryTemplateBuilder().setJobExecutionId(jobExecutionId);
        celr.getQueryMaskBuilder().addPaths("jobExecutionId");
        executionsListReply = DbService.getInstance().getExecutionsAdapter()
                .listCrawlExecutionStatus(celr.build()).stream().collect(Collectors.toList());
    }

    public List<CrawlExecutionStatus> getCrawlExecutionStatus() {
        return executionsListReply;
    }

    public CrawlExecutionStatus getCrawlExecutionStatus(String id) {
        for (CrawlExecutionStatus c : getCrawlExecutionStatus()) {
            if (c.getId().equals(id)) {
                return c;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Executions: \n");
        getCrawlExecutionStatus().forEach(v -> {
            sb.append("  ").append(formatCrawlExecution(v)).append("\n");
        });
        return sb.toString();
    }

    public static String formatCrawlExecution(CrawlExecutionStatus crawlExecutionStatus) {
        return new StringBuilder(crawlExecutionStatus.getId())
                .append(", Scope: ").append(crawlExecutionStatus.getScope().getSurtPrefix())
                .append(", State: ").append(crawlExecutionStatus.getState())
                .append(", Docs: ").append(crawlExecutionStatus.getDocumentsCrawled())
                .append(", Uris: ").append(crawlExecutionStatus.getUrisCrawled())
                .append(", Bytes: ").append(crawlExecutionStatus.getBytesCrawled())
                .append(", JobExecutionId: ").append(crawlExecutionStatus.getJobExecutionId())
                .toString();
    }
}
