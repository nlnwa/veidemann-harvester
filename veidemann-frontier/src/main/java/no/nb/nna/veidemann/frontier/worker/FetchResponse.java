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
package no.nb.nna.veidemann.frontier.worker;

import no.nb.nna.veidemann.api.MessagesProto.QueuedUri;

import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FetchResponse {
    private long bytesDownloaded;
    private int uriCount;
    private Spliterator<QueuedUri> outlinks;

    public long getBytesDownloaded() {
        return bytesDownloaded;
    }

    public FetchResponse withBytesDownloaded(long bytesDownloaded) {
        this.bytesDownloaded = bytesDownloaded;
        return this;
    }

    public int getUriCount() {
        return uriCount;
    }

    public FetchResponse withUriCount(int uriCount) {
        this.uriCount = uriCount;
        return this;
    }

    public Stream<QueuedUri> getOutlinks() {
        return StreamSupport.stream(outlinks, false);
    }

    public FetchResponse withOutlinks(Spliterator<QueuedUri> outlinks) {
        this.outlinks = outlinks;
        return this;
    }

}
