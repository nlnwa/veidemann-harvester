/*
 * Copyright 2017 National Library of Norway.
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
package no.nb.nna.broprox.controller;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import no.nb.nna.broprox.api.ControllerGrpc;
import no.nb.nna.broprox.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.broprox.api.ControllerProto.ListRequest;
import no.nb.nna.broprox.commons.DbAdapter;
import no.nb.nna.broprox.db.ProtoUtils;
import no.nb.nna.broprox.model.ConfigProto;
import no.nb.nna.broprox.model.ConfigProto.CrawlEntity;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 *
 */
public class ControllerServiceTest {

    private final String uniqueServerName = "in-process server for " + getClass();

    private InProcessServerBuilder inProcessServerBuilder;

    private ControllerApiServer inProcessServer;

    private ManagedChannel inProcessChannel;

    private ControllerGrpc.ControllerBlockingStub blockingStub;

    private ControllerGrpc.ControllerStub asyncStub;

    @Before
    public void beforeEachTest() throws InstantiationException, IllegalAccessException, IOException {
        inProcessServerBuilder = InProcessServerBuilder.forName(uniqueServerName).directExecutor();
        inProcessChannel = InProcessChannelBuilder.forName(uniqueServerName).directExecutor().build();
        blockingStub = ControllerGrpc.newBlockingStub(inProcessChannel);
        asyncStub = ControllerGrpc.newStub(inProcessChannel);
    }

    @After
    public void afterEachTest() {
        inProcessChannel.shutdownNow();
        inProcessServer.close();
    }

    @Test
    public void testSaveEntity() throws InterruptedException {
        DbAdapter dbMock = mock(DbAdapter.class);
        inProcessServer = new ControllerApiServer(inProcessServerBuilder, dbMock, null).start();

        CrawlEntity request = CrawlEntity.newBuilder()
                .setMeta(ConfigProto.Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Daily"))
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Government"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();

        CrawlEntity reply = CrawlEntity.newBuilder()
                .setId("Random UID")
                .setMeta(ConfigProto.Meta.newBuilder()
                        .setName("Nasjonalbiblioteket")
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("frequency")
                                .setValue("Daily"))
                        .addLabel(ConfigProto.Label.newBuilder()
                                .setKey("orgType")
                                .setValue("Government"))
                        .setCreated(ProtoUtils.odtToTs(OffsetDateTime.parse("2017-04-06T06:20:35.779Z"))))
                .build();

        when(dbMock.saveCrawlEntity(request)).thenReturn(reply);

        CrawlEntity response;
        response = blockingStub.saveEntity(request);
        assertThat(response).isSameAs(reply);

        final CountDownLatch latch = new CountDownLatch(1);
        asyncStub.saveEntity(request, new StreamObserver<CrawlEntity>() {
            @Override
            public void onNext(CrawlEntity value) {
                assertThat(response).isSameAs(reply);
            }

            @Override
            public void onError(Throwable t) {
                fail("An error was thrown", t);
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }

        });
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();

        verify(dbMock, times(2)).saveCrawlEntity(request);
        verifyNoMoreInteractions(dbMock);
    }

    @Test
    public void testListCrawlEntities() throws InterruptedException {
        DbAdapter dbMock = mock(DbAdapter.class);
        inProcessServer = new ControllerApiServer(inProcessServerBuilder, dbMock, null).start();

        ListRequest request = ListRequest.newBuilder().build();
        CrawlEntityListReply reply = CrawlEntityListReply.newBuilder().build();

        when(dbMock.listCrawlEntities(request)).thenReturn(reply);

        CrawlEntityListReply response;
        response = blockingStub.listCrawlEntities(request);
        assertThat(response).isSameAs(reply);

        final CountDownLatch latch = new CountDownLatch(1);
        asyncStub.listCrawlEntities(null, new StreamObserver<CrawlEntityListReply>() {
            @Override
            public void onNext(CrawlEntityListReply value) {
                assertThat(response).isSameAs(reply);
            }

            @Override
            public void onError(Throwable t) {
                fail("An error was thrown", t);
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }

        });
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();

        verify(dbMock, times(2)).listCrawlEntities(request);
        verifyNoMoreInteractions(dbMock);
    }

}
