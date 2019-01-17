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
package no.nb.nna.veidemann.robotsservice;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import no.nb.nna.veidemann.api.robotsevaluator.v1.IsAllowedReply;
import no.nb.nna.veidemann.api.robotsevaluator.v1.IsAllowedRequest;
import no.nb.nna.veidemann.api.robotsevaluator.v1.RobotsEvaluatorGrpc;
import no.nb.nna.veidemann.robotsparser.RobotsTxtParser;
import org.netpreserve.commons.uri.Uri;
import org.netpreserve.commons.uri.UriConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 *
 */
public class RobotsService extends RobotsEvaluatorGrpc.RobotsEvaluatorImplBase {

    private static final Logger LOG = LoggerFactory.getLogger(RobotsService.class);

    private final RobotsCache cache;

    private final RobotsTxtParser ROBOTS_TXT_PARSER = new RobotsTxtParser();

    public RobotsService(String proxyHost, int proxyPort) {
        this.cache = new RobotsCache(proxyHost, proxyPort);
    }

    @Override
    public void isAllowed(IsAllowedRequest request, StreamObserver<IsAllowedReply> respObserver) {
        Objects.requireNonNull(request.getExecutionId());
        Objects.requireNonNull(request.getJobExecutionId());
        Objects.requireNonNull(request.getPoliteness());
        Objects.requireNonNull(request.getUnknownFields());
        Objects.requireNonNull(request.getUserAgent());
        Objects.requireNonNull(request.getCollectionRef());
        try {
            Uri uri = UriConfigs.WHATWG.buildUri(request.getUri());
            int ttlSeconds = request.getPoliteness().getPolitenessConfig().getMinimumRobotsValidityDurationS();
            boolean allowed = false;

            switch (request.getPoliteness().getPolitenessConfig().getRobotsPolicy()) {
                case OBEY_ROBOTS:
                    allowed = cache.get(uri, ttlSeconds, request.getExecutionId(), request.getJobExecutionId(), request.getCollectionRef().getId())
                            .isAllowed(request.getUserAgent(), uri);
                    break;
                case IGNORE_ROBOTS:
                    allowed = true;
                    break;
                case CUSTOM_ROBOTS:
                    allowed = ROBOTS_TXT_PARSER.parse(request.getPoliteness().getPolitenessConfig().getCustomRobots())
                            .isAllowed(request.getUserAgent(), uri);
                    break;
                default:
                    LOG.warn("Robots Policy '{}' is not implemented.", request.getPoliteness()
                            .getPolitenessConfig().getRobotsPolicy());
                    allowed = true;
                    break;
            }

            IsAllowedReply reply = IsAllowedReply.newBuilder().setIsAllowed(allowed).build();
            respObserver.onNext(reply);
            respObserver.onCompleted();
        } catch (Exception ex) {
            LOG.error(ex.getMessage(), ex);
            Status status = Status.UNKNOWN.withDescription(ex.toString());
            respObserver.onError(status.asException());
        }
    }

}
