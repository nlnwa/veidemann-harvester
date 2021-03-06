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
package no.nb.nna.veidemann.harvester.browsercontroller;

import no.nb.nna.veidemann.api.commons.v1.Error;
import no.nb.nna.veidemann.api.frontier.v1.QueuedUri;

import java.util.List;
import java.util.stream.Stream;

public class RenderResult {
    private long bytesDownloaded;
    private int uriCount;
    private List<QueuedUri> outlinks;
    private Error error;
    private long pageFetchTimeMs;

    public long getBytesDownloaded() {
        return bytesDownloaded;
    }

    public RenderResult withBytesDownloaded(long bytesDownloaded) {
        this.bytesDownloaded = bytesDownloaded;
        return this;
    }

    public int getUriCount() {
        return uriCount;
    }

    public RenderResult withUriCount(int uriCount) {
        this.uriCount = uriCount;
        return this;
    }

    public Stream<QueuedUri> getOutlinks() {
        if (outlinks == null) {
            return Stream.empty();
        }
        return outlinks.stream();
    }

    public RenderResult withOutlinks(List<QueuedUri> outlinks) {
        this.outlinks = outlinks;
        return this;
    }

    public Error getError() {
        return error;
    }

    public boolean hasError() {
        return error != null;
    }

    public RenderResult withError(Error error) {
        this.error = error;
        return this;
    }

    public long getPageFetchTimeMs() {
        return pageFetchTimeMs;
    }

    public RenderResult withPageFetchTimeMs(long pageFetchTimeMs) {
        this.pageFetchTimeMs = pageFetchTimeMs;
        return this;
    }
}
