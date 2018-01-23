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
package no.nb.nna.veidemann.contentwriter.warc;

import no.nb.nna.veidemann.api.ContentWriterProto.WriteRequestMeta;
import no.nb.nna.veidemann.contentwriter.Util;
import no.nb.nna.veidemann.db.ProtoUtils;
import org.jwat.warc.WarcFileNaming;
import org.jwat.warc.WarcFileNamingDefault;
import org.jwat.warc.WarcFileWriter;
import org.jwat.warc.WarcFileWriterConfig;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;

import static org.jwat.warc.WarcConstants.*;

/**
 *
 */
public class SingleWarcWriter implements AutoCloseable {

    WarcFileWriter warcFileWriter;

    public SingleWarcWriter(String filePrefix, File targetDir, long maxFileSize, boolean compress, String hostName) {
        WarcFileNaming warcFileNaming = new VeidemannWarcFileNaming(filePrefix, hostName);
        WarcFileWriterConfig writerConfig = new WarcFileWriterConfig(targetDir, compress, maxFileSize, false);
        warcFileWriter = WarcFileWriter.getWarcWriterInstance(warcFileNaming, writerConfig);
    }

    public URI writeWarcHeader(String warcId, final WriteRequestMeta request,
                               final WriteRequestMeta.RecordMeta recordMeta, final List<String> allRecordIds)
            throws UncheckedIOException {

        try {
            boolean newFile = warcFileWriter.nextWriter();
            File currentFile = warcFileWriter.getFile();
            String finalFileName = currentFile.getName().substring(0, currentFile.getName().length() - 5);

            if (newFile) {
                writeFileDescriptionRecords(finalFileName);
            }

            WarcWriter writer = warcFileWriter.getWriter();

            WarcRecord record = WarcRecord.createRecord(writer);

            String recordType = Util.getRecordTypeString(recordMeta.getType());

            record.header.addHeader(FN_WARC_TYPE, recordType);
            record.header.addHeader(FN_WARC_TARGET_URI, request.getTargetUri());
            Date warcDate = Date.from(ProtoUtils.tsToOdt(request.getFetchTimeStamp()).toInstant());
            record.header.addHeader(FN_WARC_DATE, warcDate, null);
            record.header.addHeader(FN_WARC_RECORD_ID, Util.formatIdentifierAsUrn(warcId));

            if (RT_REVISIT.equals(recordType)) {
                record.header.addHeader(FN_WARC_PROFILE, PROFILE_IDENTICAL_PAYLOAD_DIGEST);
                record.header.addHeader(FN_WARC_REFERS_TO, Util.formatIdentifierAsUrn(recordMeta.getWarcRefersTo()));
            }

            record.header.addHeader(FN_WARC_IP_ADDRESS, request.getIpAddress());
            record.header.addHeader(FN_WARC_WARCINFO_ID, "<" + warcFileWriter.warcinfoRecordId + ">");
            record.header.addHeader(FN_WARC_BLOCK_DIGEST, recordMeta.getBlockDigest());
            record.header.addHeader(FN_WARC_PAYLOAD_DIGEST, recordMeta.getPayloadDigest());

            record.header.addHeader(FN_CONTENT_LENGTH, recordMeta.getSize(), null);

            if (recordMeta.getSize() > 0) {
                record.header.addHeader(FN_CONTENT_TYPE, recordMeta.getRecordContentType());
            }

            for (String otherId : allRecordIds) {
                if (!otherId.equals(warcId)) {
                    record.header.addHeader(FN_WARC_CONCURRENT_TO, otherId);
                }
            }

            writer.writeHeader(record);

            return new URI("warcfile:" + finalFileName + ":" + currentFile.length());
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public long addPayload(byte[] data) throws UncheckedIOException {
        try {
            return warcFileWriter.getWriter().writePayload(data);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public long addPayload(InputStream data) throws UncheckedIOException {
        try {
            return warcFileWriter.getWriter().streamPayload(data);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    public void closeRecord() throws IOException {
        warcFileWriter.getWriter().closeRecord();
    }

    @Override
    public void close() throws Exception {
        warcFileWriter.close();
    }

    void writeFileDescriptionRecords(String finalFileName) throws IOException {
        WarcWriter writer = warcFileWriter.getWriter();
        WarcRecord record = WarcRecord.createRecord(writer);

        record.header.addHeader(FN_WARC_TYPE, RT_WARCINFO);
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        cal.setTimeInMillis(System.currentTimeMillis());
        record.header.addHeader(FN_WARC_DATE, cal.getTime(), null);
        record.header.addHeader(FN_WARC_FILENAME, finalFileName);
        record.header.addHeader(FN_WARC_RECORD_ID, "<" + warcFileWriter.warcinfoRecordId + ">");
        record.header.addHeader(FN_CONTENT_TYPE, "application/warc-fields");
        record.header.addHeader(FN_CONTENT_LENGTH, "0");
        // Standard says no.
        //record.header.addHeader(FN_WARC_CONCURRENT_TO, "<urn:uuid:" + filedescUuid + ">");
        writer.writeHeader(record);
        writer.closeRecord();

//                managedPayload.manageVersionBlock(arcRecord, true);
//
//                contentLength = managedPayload.payloadLength;
//                warcBlockDigest = WarcDigest.createWarcDigest("SHA1", managedPayload.blockDigestBytes, "base32", Base32
//                        .encodeArray(managedPayload.blockDigestBytes));
        record = WarcRecord.createRecord(writer);
        record.header.addHeader(FN_WARC_TYPE, RT_METADATA);
//                record.header.addHeader(FN_WARC_TARGET_URI, arcRecord.header.urlUri, arcRecord.header.urlStr);
        record.header.addHeader(FN_WARC_DATE, cal.getTime(), null);
        String fileDescUuid = "<urn:uuid:" + UUID.randomUUID() + ">";
        record.header.addHeader(FN_WARC_RECORD_ID, fileDescUuid);
        record.header.addHeader(FN_WARC_CONCURRENT_TO, "<" + warcFileWriter.warcinfoRecordId + ">");
//                record.header.addHeader(FN_WARC_IP_ADDRESS, arcRecord.header.inetAddress, arcRecord.header.ipAddressStr);
        record.header.addHeader(FN_WARC_WARCINFO_ID, "<" + warcFileWriter.warcinfoRecordId + ">");
//                record.header.addHeader(FN_WARC_BLOCK_DIGEST, warcBlockDigest, null);
//                record.header.addHeader(FN_CONTENT_LENGTH, contentLength, null);
        record.header.addHeader(FN_CONTENT_LENGTH, 0, null);
        record.header.addHeader(FN_CONTENT_TYPE, "text/plain");
        writer.writeHeader(record);
//                payloadStream = managedPayload.getPayloadStream();
//                if (payloadStream != null) {
//                    writer.streamPayload(payloadStream);
//                    payloadStream.close();
//                    payloadStream = null;
//                }
        writer.closeRecord();
    }

}
