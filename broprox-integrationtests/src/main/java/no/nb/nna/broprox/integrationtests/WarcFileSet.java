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
package no.nb.nna.broprox.integrationtests;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.io.ByteStreams;
import org.jwat.warc.WarcRecord;

/**
 *
 */
public class WarcFileSet {

    private final List<WarcFile> warcFiles;

    public WarcFileSet(Stream<WarcFile> fileStream) {
        warcFiles = fileStream.collect(Collectors.toList());
    }

    public Stream<WarcFile> listFiles() {
        return warcFiles.stream();
    }

    public Stream<WarcRecord> getRecordStream() {
        Stream<WarcRecord>[] streams = listFiles()
                .map(f -> f.getContent()).collect(Collectors.toList()).toArray(new Stream[]{});
        return Stream.of(streams).flatMap(s -> s).onClose(() -> {
            for (Stream<WarcRecord> s : streams) {
                try {
                    s.close();
                } catch (Exception e) {
                    // Nothing we can do except ensure that other streams are closed
                    // even if one throws an exception.
                }
            }
        });
    }

    public Stream<WarcRecord> getContentRecordStream() {
        return getRecordStream().filter(r -> r.header.warcTargetUriStr != null);
    }

    public long getRecordCount() {
        return getContentRecordStream().count();
    }

    public void getTargetUris() {
        getContentRecordStream()
                .forEach(r -> {

                    // TODO: Should be turned into something checkable
                    System.out.println("  TYPE: " + r.header.warcTypeStr + ", URI: " + r.header.warcTargetUriStr);
                    if ("warcinfo".equals(r.header.warcTypeStr)
                            || "metadata".equals(r.header.warcTypeStr)
                            || r.header.warcTargetUriStr.endsWith("robots.txt")) {
                        System.out.println("    HE: " + new String(r.header.headerBytes));
                        if (r.hasPayload()) {
                            try {
                                System.out.println("    PL: " + new String(ByteStreams
                                        .toByteArray(r.getPayloadContent())) + "----------");
                            } catch (IOException ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                });
    }

}