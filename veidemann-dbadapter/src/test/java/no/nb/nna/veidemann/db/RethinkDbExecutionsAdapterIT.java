/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db;

import com.google.protobuf.Timestamp;
import com.rethinkdb.RethinkDB;
import no.nb.nna.veidemann.api.StatusProto.ExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.JobExecutionsListReply;
import no.nb.nna.veidemann.api.StatusProto.ListExecutionsRequest;
import no.nb.nna.veidemann.api.StatusProto.ListJobExecutionsRequest;
import no.nb.nna.veidemann.api.config.v1.CrawlScope;
import no.nb.nna.veidemann.api.frontier.v1.Cookie;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.CrawlExecutionStatusChange;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.commons.settings.CommonSettings;
import no.nb.nna.veidemann.db.initializer.RethinkDbInitializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RethinkDbExecutionsAdapterIT {

    public static RethinkDbConnection db;
    public static RethinkDbExecutionsAdapter executionsAdapter;

    static RethinkDB r = RethinkDB.r;

    @BeforeClass
    public static void init() throws DbException {
        String dbHost = System.getProperty("db.host");
        int dbPort = Integer.parseInt(System.getProperty("db.port"));

        if (!DbService.isConfigured()) {
            CommonSettings settings = new CommonSettings();
            DbService.configure(new CommonSettings()
                    .withDbHost(dbHost)
                    .withDbPort(dbPort)
                    .withDbName("veidemann")
                    .withDbUser("admin")
                    .withDbPassword(""));
        }

        try {
            DbService.getInstance().getDbInitializer().delete();
        } catch (DbException e) {
            if (!e.getMessage().matches("Database .* does not exist.")) {
                throw e;
            }
        }
        DbService.getInstance().getDbInitializer().initialize();

        executionsAdapter = (RethinkDbExecutionsAdapter) DbService.getInstance().getExecutionsAdapter();
        db = ((RethinkDbInitializer) DbService.getInstance().getDbInitializer()).getDbConnection();
    }

    @AfterClass
    public static void shutdown() {
        DbService.getInstance().close();
    }

    @Before
    public void cleanDb() throws DbException {
        for (Tables table : Tables.values()) {
            if (table != Tables.SYSTEM) {
                try {
                    db.exec("delete", r.table(table.name).delete());
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }

    @Test
    public void createCrawlExecutionStatus() throws DbException {
        JobExecutionStatus jes1 = executionsAdapter.createJobExecutionStatus("jobId1");
        CrawlExecutionStatus ces1 = executionsAdapter.createCrawlExecutionStatus(
                "jobId1", jes1.getId(), "seed1", CrawlScope.getDefaultInstance());

        assertThat(ces1.getId()).isNotEmpty();
        assertThat(ces1.getJobId()).isEqualTo("jobId1");
        assertThat(ces1.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(ces1.getSeedId()).isEqualTo("seed1");
        assertThat(ces1.getState()).isEqualTo(CrawlExecutionStatus.State.CREATED);
        assertThat(ces1.hasLastChangeTime()).isTrue();
        assertThat(ces1.hasCreatedTime()).isTrue();
        assertThat(ces1.hasStartTime()).isFalse();
        assertThat(ces1.hasEndTime()).isFalse();
        assertThat(ces1.getDocumentsCrawled()).isEqualTo(0);
        assertThat(ces1.getDocumentsDenied()).isEqualTo(0);
        assertThat(ces1.getDocumentsFailed()).isEqualTo(0);
        assertThat(ces1.getDocumentsOutOfScope()).isEqualTo(0);
        assertThat(ces1.getDocumentsRetried()).isEqualTo(0);
        assertThat(ces1.getUrisCrawled()).isEqualTo(0);
        assertThat(ces1.getBytesCrawled()).isEqualTo(0);
        assertThat(ces1.getCurrentUriIdList()).isEmpty();
    }

    @Test
    public void updateCrawlExecutionStatus() throws DbException {
        JobExecutionStatus jes1 = executionsAdapter.createJobExecutionStatus("jobId1");
        CrawlExecutionStatus ces1 = executionsAdapter.createCrawlExecutionStatus(
                "jobId1", jes1.getId(), "seed1", CrawlScope.getDefaultInstance());

        CrawlExecutionStatusChange ch = CrawlExecutionStatusChange.newBuilder()
                .setId(ces1.getId())
                .setAddDocumentsCrawled(1)
                .setAddDocumentsDenied(1)
                .setAddDocumentsFailed(1)
                .setAddDocumentsOutOfScope(1)
                .setAddDocumentsRetried(1)
                .setAddUrisCrawled(1)
                .setAddBytesCrawled(1)
                .build();
        CrawlExecutionStatus res = executionsAdapter.updateCrawlExecutionStatus(ch);
        JobExecutionStatus jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.SLEEPING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isFalse();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(1);
        assertThat(res.getDocumentsDenied()).isEqualTo(1);
        assertThat(res.getDocumentsFailed()).isEqualTo(1);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(res.getDocumentsRetried()).isEqualTo(1);
        assertThat(res.getUrisCrawled()).isEqualTo(1);
        assertThat(res.getBytesCrawled()).isEqualTo(1);
        assertThat(res.getCurrentUriIdList()).isEmpty();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(1);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(1);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(1);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(1);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(1);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(1);

        QueuedUri qUri1 = QueuedUri.newBuilder()
                .setId("qUri1")
                .setUri("http://www.example.com")
                .addCookies(Cookie.getDefaultInstance())
                .build();
        ch = CrawlExecutionStatusChange.newBuilder()
                .setId(ces1.getId())
                .setAddCurrentUri(qUri1)
                .build();
        res = executionsAdapter.updateCrawlExecutionStatus(ch);
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FETCHING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(1);
        assertThat(res.getDocumentsDenied()).isEqualTo(1);
        assertThat(res.getDocumentsFailed()).isEqualTo(1);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(res.getDocumentsRetried()).isEqualTo(1);
        assertThat(res.getUrisCrawled()).isEqualTo(1);
        assertThat(res.getBytesCrawled()).isEqualTo(1);
        assertThat(res.getCurrentUriIdList()).containsExactly(qUri1.getId());
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(1);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(1);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(1);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(1);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(1);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(1);

        QueuedUri qUri2 = QueuedUri.newBuilder()
                .setId("qUri2")
                .setUri("http://www.example.com")
                .addCookies(Cookie.getDefaultInstance())
                .setExecutionId("foo")
                .build();
        ch = CrawlExecutionStatusChange.newBuilder()
                .setId(ces1.getId())
                .setAddCurrentUri(qUri2)
                .build();
        res = executionsAdapter.updateCrawlExecutionStatus(ch);
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FETCHING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(1);
        assertThat(res.getDocumentsDenied()).isEqualTo(1);
        assertThat(res.getDocumentsFailed()).isEqualTo(1);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(res.getDocumentsRetried()).isEqualTo(1);
        assertThat(res.getUrisCrawled()).isEqualTo(1);
        assertThat(res.getBytesCrawled()).isEqualTo(1);
        assertThat(res.getCurrentUriIdList()).containsExactly(qUri1.getId(), qUri2.getId());
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(1);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(1);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(1);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(1);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(1);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(1);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(1);

        ch = CrawlExecutionStatusChange.newBuilder()
                .setId(ces1.getId())
                .setDeleteCurrentUri(qUri1)
                .setAddDocumentsCrawled(1)
                .setAddDocumentsDenied(2)
                .setAddDocumentsFailed(3)
                .setAddDocumentsOutOfScope(4)
                .setAddDocumentsRetried(5)
                .setAddUrisCrawled(6)
                .setAddBytesCrawled(7)
                .build();
        res = executionsAdapter.updateCrawlExecutionStatus(ch);
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FETCHING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(2);
        assertThat(res.getDocumentsDenied()).isEqualTo(3);
        assertThat(res.getDocumentsFailed()).isEqualTo(4);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(res.getDocumentsRetried()).isEqualTo(6);
        assertThat(res.getUrisCrawled()).isEqualTo(7);
        assertThat(res.getBytesCrawled()).isEqualTo(8);
        assertThat(res.getCurrentUriIdList()).containsExactly(qUri2.getId());
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(4);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(6);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        ch = CrawlExecutionStatusChange.newBuilder()
                .setId(ces1.getId())
                .setDeleteCurrentUri(qUri2)
                .build();
        res = executionsAdapter.updateCrawlExecutionStatus(ch);
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.SLEEPING);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(res.getDocumentsCrawled()).isEqualTo(2);
        assertThat(res.getDocumentsDenied()).isEqualTo(3);
        assertThat(res.getDocumentsFailed()).isEqualTo(4);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(res.getDocumentsRetried()).isEqualTo(6);
        assertThat(res.getUrisCrawled()).isEqualTo(7);
        assertThat(res.getBytesCrawled()).isEqualTo(8);
        assertThat(res.getCurrentUriIdList()).isEmpty();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.RUNNING);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(1);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(0);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isFalse();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(4);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(6);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        assertThatThrownBy(() -> executionsAdapter.updateCrawlExecutionStatus(CrawlExecutionStatusChange.newBuilder()
                .setId(ces1.getId())
                .setState(CrawlExecutionStatus.State.CREATED)
                .build())).isInstanceOf(IllegalArgumentException.class);

        ch = CrawlExecutionStatusChange.newBuilder()
                .setId(ces1.getId())
                .setState(CrawlExecutionStatus.State.FINISHED)
                .setEndTime(ProtoUtils.getNowTs())
                .build();
        res = executionsAdapter.updateCrawlExecutionStatus(ch);
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FINISHED);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isTrue();
        assertThat(res.getDocumentsCrawled()).isEqualTo(2);
        assertThat(res.getDocumentsDenied()).isEqualTo(3);
        assertThat(res.getDocumentsFailed()).isEqualTo(4);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(res.getDocumentsRetried()).isEqualTo(6);
        assertThat(res.getUrisCrawled()).isEqualTo(7);
        assertThat(res.getBytesCrawled()).isEqualTo(8);
        assertThat(res.getCurrentUriIdList()).isEmpty();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.FINISHED);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(1);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isTrue();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(4);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(6);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        Timestamp ts = res.getEndTime();
        ch = CrawlExecutionStatusChange.newBuilder()
                .setId(ces1.getId())
                .setState(CrawlExecutionStatus.State.ABORTED_MANUAL)
                .setEndTime(ProtoUtils.getNowTs())
                .build();
        res = executionsAdapter.updateCrawlExecutionStatus(ch);
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces1.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FINISHED);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasStartTime()).isTrue();
        assertThat(res.hasEndTime()).isTrue();
        assertThat(res.getEndTime()).isEqualTo(ts);
        assertThat(res.getDocumentsCrawled()).isEqualTo(2);
        assertThat(res.getDocumentsDenied()).isEqualTo(3);
        assertThat(res.getDocumentsFailed()).isEqualTo(4);
        assertThat(res.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(res.getDocumentsRetried()).isEqualTo(6);
        assertThat(res.getUrisCrawled()).isEqualTo(7);
        assertThat(res.getBytesCrawled()).isEqualTo(8);
        assertThat(res.getCurrentUriIdList()).isEmpty();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.FINISHED);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(1);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isTrue();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(4);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(6);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        CrawlExecutionStatus ces2 = executionsAdapter.createCrawlExecutionStatus(
                "jobId1", jes1.getId(), "seed1", CrawlScope.getDefaultInstance());

        ch = CrawlExecutionStatusChange.newBuilder()
                .setId(ces2.getId())
                .setState(CrawlExecutionStatus.State.FINISHED)
                .build();
        res = executionsAdapter.updateCrawlExecutionStatus(ch);
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces2.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FINISHED);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasEndTime()).isFalse();
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.FINISHED);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(1);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isTrue();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(4);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(6);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);

        ts = res.getEndTime();
        assertThat(ts).isEqualTo(Timestamp.getDefaultInstance());
        ch = CrawlExecutionStatusChange.newBuilder()
                .setId(ces2.getId())
                .setState(CrawlExecutionStatus.State.ABORTED_MANUAL)
                .setEndTime(ProtoUtils.getNowTs())
                .build();
        res = executionsAdapter.updateCrawlExecutionStatus(ch);
        jesRes = executionsAdapter.getJobExecutionStatus(jes1.getId());
        assertThat(res.getId()).isEqualTo(ces2.getId());
        assertThat(res.getJobId()).isEqualTo("jobId1");
        assertThat(res.getJobExecutionId()).isEqualTo(jes1.getId());
        assertThat(res.getSeedId()).isEqualTo("seed1");
        assertThat(res.getState()).isEqualTo(CrawlExecutionStatus.State.FINISHED);
        assertThat(res.hasLastChangeTime()).isTrue();
        assertThat(res.hasCreatedTime()).isTrue();
        assertThat(res.hasEndTime()).isTrue();
        assertThat(res.getEndTime()).isNotEqualTo(ts);
        assertThat(jesRes.getState()).isEqualTo(JobExecutionStatus.State.FINISHED);
        assertThat(jesRes.getExecutionsStateMap().get("SLEEPING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FETCHING")).isEqualTo(0);
        assertThat(jesRes.getExecutionsStateMap().get("FINISHED")).isEqualTo(2);
        assertThat(jesRes.hasStartTime()).isTrue();
        assertThat(jesRes.hasEndTime()).isTrue();
        assertThat(jesRes.getDocumentsCrawled()).isEqualTo(2);
        assertThat(jesRes.getDocumentsDenied()).isEqualTo(3);
        assertThat(jesRes.getDocumentsFailed()).isEqualTo(4);
        assertThat(jesRes.getDocumentsOutOfScope()).isEqualTo(5);
        assertThat(jesRes.getDocumentsRetried()).isEqualTo(6);
        assertThat(jesRes.getUrisCrawled()).isEqualTo(7);
        assertThat(jesRes.getBytesCrawled()).isEqualTo(8);
    }

    @Test
    public void listCrawlExecutionStatus() throws DbException {
        CrawlExecutionStatus ces1 = executionsAdapter.createCrawlExecutionStatus("jobId1", "jobExe1", "seedId", CrawlScope.getDefaultInstance());
        CrawlExecutionStatus ces2 = executionsAdapter.createCrawlExecutionStatus("jobId1", "jobExe1", "seedId", CrawlScope.getDefaultInstance());

        // Check crawl executions list functions
        ExecutionsListReply eList = executionsAdapter.listCrawlExecutionStatus(ListExecutionsRequest.getDefaultInstance());
        assertThat(eList.getCount()).isEqualTo(2);
        assertThat(eList.getValueCount()).isEqualTo(2);
        assertThat(eList.getValueList()).containsExactlyInAnyOrder(ces1, ces2);

        eList = executionsAdapter.listCrawlExecutionStatus(ListExecutionsRequest.newBuilder().addId(ces2.getId()).build());
        assertThat(eList.getCount()).isEqualTo(1);
        assertThat(eList.getValueCount()).isEqualTo(1);
        assertThat(eList.getValueList()).containsExactlyInAnyOrder(ces2);
    }

    @Test
    public void listJobExecutionStatus() throws DbException {
        JobExecutionStatus jes1 = executionsAdapter.createJobExecutionStatus("jobId1");
        JobExecutionStatus jes2 = executionsAdapter.createJobExecutionStatus("jobId1");

        // Check job executions list functions
        JobExecutionsListReply jList = executionsAdapter.listJobExecutionStatus(ListJobExecutionsRequest.getDefaultInstance());
        assertThat(jList.getCount()).isEqualTo(2);
        assertThat(jList.getValueCount()).isEqualTo(2);
        assertThat(jList.getValueList()).containsExactlyInAnyOrder(jes1, jes2);

        jList = executionsAdapter.listJobExecutionStatus(ListJobExecutionsRequest.newBuilder().addId(jes2.getId()).build());
        assertThat(jList.getCount()).isEqualTo(1);
        assertThat(jList.getValueCount()).isEqualTo(1);
        assertThat(jList.getValueList()).containsExactlyInAnyOrder(jes2);
    }

}