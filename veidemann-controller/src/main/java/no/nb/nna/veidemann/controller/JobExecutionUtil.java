package no.nb.nna.veidemann.controller;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.ConfigProto.CrawlJob;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.MessagesProto.JobExecutionStatus;
import no.nb.nna.veidemann.commons.util.ApiTools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static no.nb.nna.veidemann.commons.util.ApiTools.buildLabel;

public class JobExecutionUtil {
    private static final Logger LOG = LoggerFactory.getLogger(JobExecutionUtil.class);

    private final static Map<String, FrontierClient> frontierClients = new HashMap<>();

    private JobExecutionUtil() {
    }

    static void addFrontierClient(String seedType, FrontierClient client) {
        frontierClients.put(seedType.toLowerCase(), client);
    }

    /**
     * Helper method for getting one object. Sends NOT_FOUND if object is null.
     *
     * @param response the object which is checked for null
     * @param responseObserver the observer to send the object to
     */
    public static void handleGet(Object response, StreamObserver responseObserver) {
        try {
            if (response == null) {
                Status status = Status.NOT_FOUND;
                responseObserver.onError(status.asException());
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            responseObserver.onError(status.asException());
        }
    }

    public static void crawlSeed(CrawlJob job, Seed seed, JobExecutionStatus jobExecutionStatus) {
        if (!seed.getDisabled()) {
            LOG.info("Start harvest of: {}", seed.getMeta().getName());

            String type = ApiTools.getFirstLabelWithKey(seed.getMeta(), "type")
                    .orElse(buildLabel("type", "url")).getValue().toLowerCase();

            FrontierClient frontierClient = frontierClients.get(type);

            if (frontierClient != null) {
                frontierClient.crawlSeed(job, seed, jobExecutionStatus);
            } else {
                LOG.warn("No frontier defined for seed type {}", type);
            }
        }
    }
}
