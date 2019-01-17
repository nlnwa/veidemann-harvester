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
package no.nb.nna.veidemann.commons.db;

import no.nb.nna.veidemann.api.StatusProto.ExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.JobExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.ListExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.ListJobExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.RunningExecutionsRequest;
import no.nb.nna.veidemann.api.config.v1.CrawlScope;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatusChange;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;

public interface ExecutionsAdapter {
    JobExecutionStatus createJobExecutionStatus(String jobId) throws DbException;

    JobExecutionStatus getJobExecutionStatus(String jobExecutionId) throws DbException;

    JobExecutionsListReply listJobExecutionStatus(ListJobExecutionsRequest request) throws DbException;

    /**
     * Update the state for a Job Execution to ABORTED_MANUAL.
     *
     * @param jobExecutionId id of the job execution to update
     */
    JobExecutionStatus setJobExecutionStateAborted(String jobExecutionId) throws DbException;

    CrawlExecutionStatus createCrawlExecutionStatus(String jobId, String jobExecutionId, String seedId, CrawlScope scope) throws DbException;

    CrawlExecutionStatus updateCrawlExecutionStatus(CrawlExecutionStatusChange status) throws DbException;

    CrawlExecutionStatus getCrawlExecutionStatus(String crawlExecutionId) throws DbException;

    ExecutionsListReply listCrawlExecutionStatus(ListExecutionsRequest request) throws DbException;

    /**
     * Update the state for a Crawl Execution to ABORTED_MANUAL.
     * <p>
     * The frontier should detect this and abort the crawl.
     *
     * @param crawlExecutionId id of the execution to update
     */
    CrawlExecutionStatus setCrawlExecutionStateAborted(String crawlExecutionId) throws DbException;

    ChangeFeed<RunningExecutionsListReply> getCrawlExecutionStatusStream(RunningExecutionsRequest request) throws DbException;

}
