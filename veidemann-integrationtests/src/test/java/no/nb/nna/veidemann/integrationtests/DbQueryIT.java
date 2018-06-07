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

import io.grpc.Status;
import no.nb.nna.veidemann.api.ReportProto.ExecuteDbQueryRequest;
import no.nb.nna.veidemann.commons.VeidemannHeaderConstants;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbHelper;
import org.junit.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class DbQueryIT extends CrawlTestBase implements VeidemannHeaderConstants {

    @Test
    public void testHarvest() throws InterruptedException, ExecutionException, DbException {
        DbHelper dbh = DbHelper.getInstance();
        dbh.configure(db);

        QueryObserver observer = new QueryObserver();
        reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                .setQuery("r.table('config_seeds').count()").build(), observer);
        observer.await();
        assertThat(observer.getResults()).containsExactly("0");

        for (int i = 0; i < 4; i++) {
            QueryObserver insertObserver = new QueryObserver();
            reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                    .setQuery("r.table('config_seeds').insert({foo:'bar'})").build(), insertObserver);
            insertObserver.await();
        }

        observer = new QueryObserver();
        reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                .setQuery("r.table('config_seeds').count()").build(), observer);
        observer.await();
        assertThat(observer.getResults()).containsExactly("4");

        observer = new QueryObserver();
        reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                .setQuery("r.table('config_seeds')").build(), observer);
        observer.await();
        assertThat(observer.getResults()).hasSize(4);

        observer = new QueryObserver();
        reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                .setQuery("r.table('config_seeds')").setLimit(1).build(), observer);
        observer.await();
        assertThat(observer.getResults()).hasSize(1);

        observer = new QueryObserver();
        reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                .setQuery("r.table('config_seeds').count()").build(), observer);
        observer.await();
        assertThat(observer.getResults()).containsExactly("4");
    }

    @Test
    public void testClientCancel() throws InterruptedException, ExecutionException, DbException {
        DbHelper dbh = DbHelper.getInstance();
        dbh.configure(db);

        QueryObserver observer = new QueryObserver();
        reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                .setQuery("r.table('config_seeds').changes()").build(), observer);
        Thread.sleep(500);
        for (int i = 0; i < 10; i++) {
            QueryObserver insertObserver = new QueryObserver();
            reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                    .setQuery("r.table('config_seeds').insert({foo:'bar'})").build(), insertObserver);
            insertObserver.await();
        }
        Thread.sleep(500);
        observer.cancel();
        for (int i = 0; i < 10; i++) {
            QueryObserver insertObserver = new QueryObserver();
            reportClient.executeDbQuery(ExecuteDbQueryRequest.newBuilder()
                    .setQuery("r.table('config_seeds').insert({foo:'bar'})").build(), insertObserver);
            insertObserver.await();
        }
        observer.await();
        assertThat(observer.getResults()).hasSize(10);
        assertThat(observer.getException()).isNotNull();
        assertThat(observer.getException().getStatus().getCode()).isEqualTo(Status.CANCELLED.getCode());
    }
}
