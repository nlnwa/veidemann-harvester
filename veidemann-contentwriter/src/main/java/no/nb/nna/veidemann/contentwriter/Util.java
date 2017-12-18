package no.nb.nna.veidemann.contentwriter;

import no.nb.nna.veidemann.api.ContentWriterProto.RecordType;

import java.util.UUID;

public class Util {
    private Util() {
    }

    public static String createIdentifier() {
        return UUID.randomUUID().toString();
    }

    public static String formatIdentifierAsUrn(String id) {
        return "<urn:uuid:" + id + ">";
    }

    public static String getRecordTypeString(RecordType recordType) {
        return recordType.name().toLowerCase();
    }
}
