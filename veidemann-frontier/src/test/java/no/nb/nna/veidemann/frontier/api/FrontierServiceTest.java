package no.nb.nna.veidemann.frontier.api;

import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import no.nb.nna.veidemann.api.ConfigProto.CrawlConfig;
import no.nb.nna.veidemann.api.FrontierProto.PageHarvestSpec;
import no.nb.nna.veidemann.api.MessagesProto.Error;
import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.frontier.worker.CrawlExecution;
import no.nb.nna.veidemann.frontier.worker.Frontier;
import no.nb.nna.veidemann.harvester.FrontierClient;
import no.nb.nna.veidemann.harvester.browsercontroller.BrowserController;
import no.nb.nna.veidemann.harvester.browsercontroller.RenderResult;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class FrontierServiceTest {
    private final String uniqueServerName = "in-process server for " + getClass();

    @After
    public void afterEachTest() {
        validateMockitoUsage();
    }

    @Test
    @Ignore
    public void crawlSeed() {
    }

    @Test
    public void getNextPage() throws InterruptedException, ExecutionException, IOException, TimeoutException, DbException {
        Frontier frontierMock = mock(Frontier.class);
        BrowserController controller = mock(BrowserController.class);
        CrawlExecution crawlExecutionMock = mock(CrawlExecution.class);

        InProcessServerBuilder inProcessServerBuilder = InProcessServerBuilder.forName(uniqueServerName);
        InProcessChannelBuilder inProcessChannelBuilder = InProcessChannelBuilder.forName(uniqueServerName);
        FrontierApiServer inProcessServer = new FrontierApiServer(inProcessServerBuilder, frontierMock).start();

        FrontierClient client = new FrontierClient(controller, inProcessChannelBuilder, 2, "", 0);

        PageHarvestSpec.Builder harvestSpec = PageHarvestSpec.newBuilder();
        harvestSpec.getCrawlConfigBuilder().setId("foo");

        QueuedUri uri1 = QueuedUri.newBuilder().setUri("http://example1.com").build();
        QueuedUri uri2 = QueuedUri.newBuilder().setUri("http://example2.com").build();
        PageHarvestSpec harvestSpec1 = harvestSpec.setQueuedUri(uri1).build();
        PageHarvestSpec harvestSpec2 = harvestSpec.setQueuedUri(uri2).build();

        when(frontierMock.getNextPageToFetch()).thenReturn(crawlExecutionMock);

        when(crawlExecutionMock.preFetch()).thenReturn(harvestSpec1, harvestSpec2);
        doThrow(new RuntimeException("Simulated exception in postFetchSuccess")).when(crawlExecutionMock).postFetchSuccess(any());

        when(controller.render(any(), any(QueuedUri.class), any(CrawlConfig.class)))
                .thenReturn(new RenderResult())
                .thenThrow(new RuntimeException("Simulated render exception"))
                .thenReturn(new RenderResult().withError(Error.newBuilder().setCode(1).setMsg("Error").build()));

        client.requestNextPage();
        client.requestNextPage();
        client.requestNextPage();

        client.close();
        inProcessServer.close();
        inProcessServer.blockUntilShutdown();

        verify(frontierMock, times(3)).getNextPageToFetch();
        verify(controller, times(3)).render(any(), any(QueuedUri.class), any(CrawlConfig.class));
        verify(crawlExecutionMock, times(3)).preFetch();
        verify(crawlExecutionMock, times(3)).postFetchFinally();
        verify(crawlExecutionMock).postFetchSuccess(any());
        verify(crawlExecutionMock).postFetchFailure(any(Throwable.class));
        verify(crawlExecutionMock).postFetchFailure(any(Error.class));
        verifyNoMoreInteractions(frontierMock, controller, crawlExecutionMock);
    }
}