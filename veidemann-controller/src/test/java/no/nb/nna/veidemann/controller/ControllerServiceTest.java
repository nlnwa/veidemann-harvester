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
package no.nb.nna.veidemann.controller;

import com.google.common.collect.ImmutableList;
import com.nimbusds.jwt.JWTClaimsSet;
import io.grpc.Attributes;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import net.minidev.json.JSONArray;
import no.nb.nna.veidemann.api.ControllerGrpc;
import no.nb.nna.veidemann.api.ControllerProto.CrawlEntityListReply;
import no.nb.nna.veidemann.api.ControllerProto.ListRequest;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.commons.auth.AuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.EmailContextKey;
import no.nb.nna.veidemann.commons.auth.IdTokenAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.IdTokenValidator;
import no.nb.nna.veidemann.commons.auth.NoopAuAuServerInterceptor;
import no.nb.nna.veidemann.commons.auth.UserRoleMapper;
import no.nb.nna.veidemann.controller.settings.Settings;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.api.ConfigProto;
import no.nb.nna.veidemann.api.ConfigProto.CrawlEntity;
import no.nb.nna.veidemann.api.ConfigProto.Role;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
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

    private Settings settings = new Settings();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

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
        AuAuServerInterceptor auau = new NoopAuAuServerInterceptor();
        inProcessServer = new ControllerApiServer(settings, inProcessServerBuilder, dbMock, null, auau).start();

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
        AuAuServerInterceptor auau = new NoopAuAuServerInterceptor();
        inProcessServer = new ControllerApiServer(settings, inProcessServerBuilder, dbMock, null, auau).start();

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

    @Test
    public void testAuthorization() {
        CallCredentials cred = new CallCredentials() {
            @Override
            public void applyRequestMetadata(MethodDescriptor<?, ?> method, Attributes attrs, Executor appExecutor, MetadataApplier applier) {
                Metadata headers = new Metadata();
                headers.put(IdTokenAuAuServerInterceptor.BEARER_TOKEN_KEY, "Bearer token1");
                applier.apply(headers);
            }

            @Override
            public void thisUsesUnstableApi() {

            }
        };

        DbAdapter dbMock = mock(DbAdapter.class);
        IdTokenValidator idValidatorMock = mock(IdTokenValidator.class);
        UserRoleMapper roleMapperMock = mock(UserRoleMapper.class);
        AuAuServerInterceptor auau = new IdTokenAuAuServerInterceptor(roleMapperMock, idValidatorMock);
        inProcessServer = new ControllerApiServer(settings, inProcessServerBuilder, dbMock, null, auau).start();

        when(dbMock.listCrawlEntities(ListRequest.getDefaultInstance()))
                .thenReturn(CrawlEntityListReply.getDefaultInstance());
        when(dbMock.saveCrawlEntity(CrawlEntity.getDefaultInstance()))
                .thenAnswer(new Answer<CrawlEntity>() {
            @Override
            public CrawlEntity answer(InvocationOnMock invocation) throws Throwable {
                assertThat(EmailContextKey.email()).isEqualTo("user@example.com");
                return CrawlEntity.getDefaultInstance();
            }
        });
        when(idValidatorMock.verifyIdToken("token1"))
                .thenReturn(new JWTClaimsSet.Builder()
                        .claim("email", "user@example.com")
                        .claim("groups", new JSONArray())
                        .build());
        when(roleMapperMock.getRolesForUser(eq("user@example.com"), anyList(), anyCollection()))
                .thenReturn(ImmutableList.of(Role.READONLY));


        assertThat(blockingStub.withCallCredentials(cred).listCrawlEntities(ListRequest.getDefaultInstance()))
                .isSameAs(CrawlEntityListReply.getDefaultInstance());


        thrown.expect(StatusRuntimeException.class);
        thrown.expectMessage("PERMISSION_DENIED");

        blockingStub.withCallCredentials(cred).saveEntity(CrawlEntity.getDefaultInstance());

        thrown.expectMessage("UNAUTHENTICATED");
        blockingStub.saveEntity(CrawlEntity.getDefaultInstance());
    }
}
