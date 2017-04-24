/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package no.nb.nna.broprox.contentwriter.text;

import java.io.IOException;
import java.io.InputStream;

import no.nb.nna.broprox.db.DbAdapter;
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
public class TextExtracter {

    public void analyze(InputStream in, CrawlLog logEntry, DbAdapter db) throws IOException {
        if (shouldParse(logEntry)) {
            AutoDetectParser parser = new AutoDetectParser();
            Metadata metadata = new Metadata();
            SkipSpaceContentHandler innerHandler = new SkipSpaceContentHandler(metadata);
            ContentHandler handler = new BodyContentHandler(innerHandler);
            try {
                parser.parse(in, handler, metadata);
                if (metadata.get("Language") != null) {
                    metadata.add("Orig-Content-Type", logEntry.getContentType());
                    metadata.add("warc-id", logEntry.getWarcId());
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

}
