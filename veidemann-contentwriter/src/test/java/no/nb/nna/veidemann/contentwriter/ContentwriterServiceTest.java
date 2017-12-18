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
package no.nb.nna.veidemann.contentwriter;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import no.nb.nna.veidemann.api.MessagesProto.CrawlLog;
import no.nb.nna.veidemann.commons.client.ContentWriterClient;
import no.nb.nna.veidemann.commons.client.ContentWriterClient.ContentWriterSession;
import no.nb.nna.veidemann.commons.db.DbAdapter;
import no.nb.nna.veidemann.contentwriter.text.TextExtractor;
import no.nb.nna.veidemann.contentwriter.warc.SingleWarcWriter;
import no.nb.nna.veidemann.contentwriter.warc.WarcWriterPool;
import no.nb.nna.veidemann.contentwriter.warc.WarcWriterPool.PooledWarcWriter;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;
import java.net.URISyntaxException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.*;

/**
 *
 */
public class ContentwriterServiceTest {

    private final String uniqueServerName = "in-process server for " + getClass();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSaveEntity() throws StatusException, InterruptedException, URISyntaxException {
        DbAdapter dbMock = mock(DbAdapter.class);
        WarcWriterPool warcWriterPoolMock = mock(WarcWriterPool.class);
        PooledWarcWriter pooledWarcWriterMock = mock(PooledWarcWriter.class);
        SingleWarcWriter singleWarcWriterMock = mock(SingleWarcWriter.class);
        TextExtractor textExtractorMock = mock(TextExtractor.class);

        InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(uniqueServerName).directExecutor();
        ManagedChannelBuilder inProcessChannelBuilder = InProcessChannelBuilder.forName(uniqueServerName).directExecutor();
        try (ApiServer inProcessServer = new ApiServer(inProcessServerBuilder, dbMock, warcWriterPoolMock, textExtractorMock).start();
             ContentWriterClient client = new ContentWriterClient(inProcessChannelBuilder);) {

            when(warcWriterPoolMock.borrow()).thenReturn(pooledWarcWriterMock);
            when(pooledWarcWriterMock.getWarcWriter()).thenReturn(singleWarcWriterMock);
//            when(singleWarcWriterMock.writeWarcHeader(any())).thenReturn(new URI("foo:bar"));
//
//            ContentWriterSession session1 = client.createSession();
//            ContentWriterSession session2 = client.createSession();
//
//            session1.sendHeader(ByteString.copyFromUtf8("head1"));
//            session2.sendHeader(ByteString.copyFromUtf8("head2"));
//            session1.sendCrawlLog(CrawlLog.getDefaultInstance());
//            session2.sendCrawlLog(CrawlLog.getDefaultInstance());
//            session1.finish();
//            session2.finish();
        }
    }

}
