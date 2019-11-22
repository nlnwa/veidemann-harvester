package no.nb.nna.veidemann.integrationtests;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Cursor;
import no.nb.nna.veidemann.api.controller.v1.ControllerGrpc;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlReply;
import no.nb.nna.veidemann.api.controller.v1.RunCrawlRequest;
import no.nb.nna.veidemann.api.frontier.v1.JobExecutionStatus;
import no.nb.nna.veidemann.api.report.v1.CrawlExecutionsListRequest;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConnection;
import no.nb.nna.veidemann.db.Tables;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RunnableFuture;
import java.util.stream.StreamSupport;

public class JobCompletion extends ForkJoinTask<JobExecutionStatus> implements RunnableFuture<JobExecutionStatus> {
    static RethinkDB r = RethinkDB.r;

    final String jobExecutionId;

    final List<String> eIds;

    JobExecutionStatus result;

    RethinkDbConnection db;

    public static JobCompletion executeJob(RethinkDbConnection db,
                                           ControllerGrpc.ControllerBlockingStub controllerClient,
                                           RunCrawlRequest crawlRequest) throws DbException {
        return (JobCompletion) ForkJoinPool.commonPool().submit(
                (ForkJoinTask) new JobCompletion(db, controllerClient, crawlRequest));
    }

    JobCompletion(RethinkDbConnection db,
                  ControllerGrpc.ControllerBlockingStub controllerClient, RunCrawlRequest request) throws DbException {
        this.db = db;

        RunCrawlReply crawlReply = controllerClient.runCrawl(request);

        jobExecutionId = crawlReply.getJobExecutionId();
        eIds = new ArrayList<>();

        CrawlExecutionsListRequest.Builder celr = CrawlExecutionsListRequest.newBuilder();
        celr.getQueryTemplateBuilder().setJobExecutionId(jobExecutionId);
        celr.getQueryMaskBuilder().addPaths("jobExecutionId");
        DbService.getInstance().getExecutionsAdapter().listCrawlExecutionStatus(celr.build()).stream()
                .forEach(jes -> eIds.add(jes.getId()));
    }

    @Override
    public JobExecutionStatus getRawResult() {
        return result;
    }

    @Override
    protected void setRawResult(JobExecutionStatus value) {
        result = value;
    }

    @Override
    public void run() {
        invoke();
    }

    @Override
    protected boolean exec() {
        try (Cursor<Map<String, Object>> cursor = db.exec("list", r.table(Tables.JOB_EXECUTIONS.name)
                .get(jobExecutionId)
                .changes().optArg("include_initial", true))) {

            StreamSupport.stream(cursor.spliterator(), false)
                    .filter(e -> e.containsKey("new_val"))
                    .map(e -> ProtoUtils
                            .rethinkToProto((Map<String, Object>) e.get("new_val"), JobExecutionStatus.class))
                    .forEach(e -> {
                        if (e.hasEndTime()) {
                            System.out.println("Job completed");
                            cursor.close();
                        }
                    });

            setRawResult(DbService.getInstance().getExecutionsAdapter().getJobExecutionStatus(jobExecutionId));
            return true;
        } catch (Error err) {
            throw err;
        } catch (RuntimeException rex) {
            throw rex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
