package no.nb.nna.veidemann.commons.util;

import com.google.protobuf.ByteString;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Sha1Digest {
    private final MessageDigest digest;
    private String digestString;
    private boolean dirty = true;

    public Sha1Digest() {
        try {
            this.digest = MessageDigest.getInstance("sha1");
        } catch (NoSuchAlgorithmException e) {
            // Should never happen because there is a requirement for all JRE implementations to include SHA1
            throw new RuntimeException(e);
        }
    }

    private Sha1Digest(MessageDigest digest) {
        this.digest = digest;
    }

    public Sha1Digest update(ByteString data) {
        dirty = true;
        for (ByteBuffer b : data.asReadOnlyByteBufferList()) {
            digest.update(b);
        }
        return this;
    }

    public Sha1Digest update(ByteBuffer data) {
        dirty = true;
        digest.update(data);
        return this;
    }

    public Sha1Digest update(char... data) {
        dirty = true;
        for (char b : data) {
            digest.update((byte) b);
        }
        return this;
    }

    public Sha1Digest update(byte... data) {
        dirty = true;
        digest.update(data);
        return this;
    }

    @Override
    public String toString() {
        return getPrefixedDigestString();
    }

    public String getPrefixedDigestString() {
        return "sha1:" + getRawDigestString();
    }

    public String getRawDigestString() {
        if (dirty) {
            try {
                MessageDigest soFar = (MessageDigest) digest.clone();
                digestString = String.format("%040x", new BigInteger(1, ((MessageDigest) digest.clone()).digest()));
                dirty = false;
            } catch (CloneNotSupportedException cnse) {
                throw new RuntimeException("Couldn't make digest of partial content");
            }
        }
        return digestString;
    }

    @Override
    public Sha1Digest clone() {
        try {
            return new Sha1Digest((MessageDigest) digest.clone());
        } catch (CloneNotSupportedException cnse) {
            throw new RuntimeException("Couldn't make digest of partial content");
        }
    }
}
