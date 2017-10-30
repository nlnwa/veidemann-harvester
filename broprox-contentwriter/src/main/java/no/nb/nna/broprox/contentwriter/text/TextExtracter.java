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
package no.nb.nna.broprox.contentwriter.text;

import java.io.IOException;
import java.io.InputStream;

import no.nb.nna.broprox.commons.db.DbAdapter;
import no.nb.nna.broprox.model.MessagesProto.CrawlLog;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 *
 */
public class TextExtracter implements AutoCloseable {

    public void analyze(InputStream in, CrawlLog logEntry, DbAdapter db) throws IOException {
        if (shouldParse(logEntry)) {
            AutoDetectParser parser = new AutoDetectParser();

            Metadata metadata = new Metadata();
            metadata.add("warc-id", logEntry.getWarcId());

            SkipSpaceContentHandler innerHandler = new SkipSpaceContentHandler(metadata);
            ContentHandler handler = new BodyContentHandler(innerHandler);
            try {
                parser.parse(in, handler, metadata);
                if (metadata.get("Language") != null) {
                    metadata.add("Orig-Content-Type", logEntry.getContentType());
//                    stats.log(logEntry.getRequestedUri(), metadata, innerHandler.getText());
                }
                System.out.println("META: " + metadata);
                if (innerHandler.getExtractedText().getCharacterCount() > 50) {
                    db.addExtractedText(innerHandler.getExtractedText());
                }
            } catch (SAXException | TikaException ex) {
                System.out.println("Failed reading content from " + logEntry.getRequestedUri() + " ("
                        + ex.getClass().getName() + ": " + ex.getCause() + ")");
                ex.printStackTrace();
            }
        }
    }

    boolean shouldParse(CrawlLog logEntry) {
        if ("response".equals(logEntry.getRecordType())) {
            String contentType = logEntry.getContentType();
            int responseCode = logEntry.getStatusCode();
            boolean shouldParse = true;

            if (contentType != null
                    && (contentType.startsWith("image/")
                    || contentType.startsWith("video/")
                    || contentType.startsWith("application/x-shockwave-flash")
                    || contentType.startsWith("application/binary")
                    || contentType.startsWith("application/rss")
                    || contentType.startsWith("application/javascript")
                    || contentType.startsWith("text/javascript")
                    || contentType.startsWith("application/x-javascript")
                    || contentType.startsWith("text/css"))) {
                shouldParse = false;
            }
            if (responseCode >= 300) {
                shouldParse = false;
            }

            if (logEntry.getRequestedUri().startsWith("dns:")
                    || logEntry.getRequestedUri().endsWith("robots.txt")) {
                shouldParse = false;
            }
            return shouldParse;
        }
        return false;
    }

    @Override
    public void close() {
    }

}
