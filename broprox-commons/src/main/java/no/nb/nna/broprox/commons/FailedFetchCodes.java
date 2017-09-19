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
package no.nb.nna.broprox.commons;

import no.nb.nna.broprox.model.MessagesProto;

/**
 *
 * @author John Erik Halse
 */
public enum FailedFetchCodes {
    SUCCESSFUL_DNS(1, "Successful DNS lookup"),
    NEVER_TRIED(0, "Fetch never tried (perhaps protocol unsupported or illegal URI)"),
    FAILED_DNS(-1, "DNS lookup failed"),
    CONNECT_FAILED(-2, "HTTP connect failed"),
    CONNECT_BROKEN(-3, "HTTP connect broken"),
    HTTP_TIMEOUT(-4, "HTTP timeout"),
    RUNTIME_EXCEPTION(-5, "Unexpected runtime exception.  See runtime-errors.log."),
    DOMAIN_LOOKUP_FAILED(-6, "Prerequisite domain-lookup failed, precluding fetch attempt."),
    ILLEGAL_URI(-7, "URI recognized as unsupported or illegal."),
    RETRY_LIMIT_REACHED(-8, "Multiple retries failed, retry limit reached."),
    FAILED_FETCHING_ROBOTS(-61, "Prerequisite robots.txt fetch failed, precluding a fetch attempt."),
    EMPTY_RESPONSE(-404, "Empty HTTP response interpreted as a 404."),
    SEVERE(-3000, "Severe Java Error condition occured such as OutOfMemoryError or StackOverflowError during URI processing."),
    CHAFF_DETECTION(-4000, "Chaff detection of traps/content with negligible value applied."),
    TOO_MANY_HOPS(-4001, "The URI is too many link hops away from the seed."),
    TOO_MANY_TRANSITIVE_HOPS(-4002, "The URI is too many embed/transitive hops away from the last URI in scope."),
    PRECLUDED_BY_SCOPE_CHANGE(-5000, "The URI is out of scope upon reexamination.  This only happens if the scope changes during the crawl."),
    BLOCKED(-5001, "Blocked from fetch by user setting."),
    BLOCKEC_BY_CUSTOM_PROCESSOR(-5002, "Blocked by a custom processor."),
    QUOTA_EXCEEDED(-5003, "Blocked due to exceeding an established quota."),
    RUNTIME_EXCEEDED(-5004, "Blocked due to exceeding an established runtime"),
    DELETED_FROM_FRONTIER(-6000, "Deleted from Frontier by user."),
    PRECLUDED_BY_ROBOTS(-9998, "Robots.txt rules precluded fetch.");

    final int code;

    final String description;

    private FailedFetchCodes(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    public MessagesProto.FetchError toFetchError() {
        return MessagesProto.FetchError.newBuilder().setCode(code).build();
    }

    public MessagesProto.FetchError toFetchError(String message) {
        return MessagesProto.FetchError.newBuilder().setCode(code).setMsg(message).build();
    }

}
