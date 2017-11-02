package no.nb.nna.broprox.commons.util;

import com.google.protobuf.ByteString;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

public class Sha1DigestTest {
    @Test
    public void getDigestString() throws Exception {
        Sha1Digest d = new Sha1Digest();
        assertThat(d).hasToString("sha1:da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    @Test
    public void updateByteString() throws Exception {
        ByteString v1 = ByteString.copyFromUtf8("foo");
        ByteString v2 = ByteString.copyFromUtf8("\r");
        ByteString v3 = ByteString.copyFromUtf8("\n");
        ByteString v4 = v1.concat(v2).concat(v3);

        Sha1Digest d = new Sha1Digest();
        d.update(v1);
        assertThat(d).hasToString("sha1:0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33");
        d.update(v2);
        assertThat(d).hasToString("sha1:95e0c0e09be59e04eb0e312e5daa11a2a830e526");
        d.update(v3);
        assertThat(d).hasToString("sha1:855426068ee8939df6bce2c2c4b1e7346532a133");

        d = new Sha1Digest();
        d.update(v4);
        assertThat(d).hasToString("sha1:855426068ee8939df6bce2c2c4b1e7346532a133");
    }

    @Test
    public void updateByteBuffer() throws Exception {
        ByteBuffer v1 = ByteBuffer.wrap("foo".getBytes());
        ByteBuffer v2 = ByteBuffer.wrap("\r".getBytes());
        ByteBuffer v3 = ByteBuffer.wrap("\n".getBytes());
        ByteBuffer v4 = ByteBuffer.allocate(50).put("foo".getBytes()).put((byte) 0x0D).put((byte) '\n');
        v4.flip();

        Sha1Digest d = new Sha1Digest();
        d.update(v1);
        assertThat(d).hasToString("sha1:0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33");
        d.update(v2);
        assertThat(d).hasToString("sha1:95e0c0e09be59e04eb0e312e5daa11a2a830e526");
        d.update(v3);
        assertThat(d).hasToString("sha1:855426068ee8939df6bce2c2c4b1e7346532a133");

        d = new Sha1Digest();
        d.update(v4);
        assertThat(d).hasToString("sha1:855426068ee8939df6bce2c2c4b1e7346532a133");
    }

    @Test
    public void updateByte() throws Exception {
        byte[] v1 = {'f', 'o', 'o'};
        byte v2 = '\r';
        byte v3 = '\n';
        byte[] v4 = {'f', 'o', 'o', v2, v3};

        Sha1Digest d = new Sha1Digest();
        d.update(v1);
        assertThat(d).hasToString("sha1:0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33");
        d.update(v2);
        assertThat(d).hasToString("sha1:95e0c0e09be59e04eb0e312e5daa11a2a830e526");
        d.update(v3);
        assertThat(d).hasToString("sha1:855426068ee8939df6bce2c2c4b1e7346532a133");

        d = new Sha1Digest();
        d.update(v4);
        assertThat(d).hasToString("sha1:855426068ee8939df6bce2c2c4b1e7346532a133");
    }

    @Test
    public void updateChar() throws Exception {
        char[] v1 = {'f', 'o', 'o'};
        char v2 = '\r';
        char v3 = '\n';
        char[] v4 = {'f', 'o', 'o', v2, v3};

        Sha1Digest d = new Sha1Digest();
        d.update(v1);
        assertThat(d).hasToString("sha1:0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33");
        d.update(v2);
        assertThat(d).hasToString("sha1:95e0c0e09be59e04eb0e312e5daa11a2a830e526");
        d.update(v3);
        assertThat(d).hasToString("sha1:855426068ee8939df6bce2c2c4b1e7346532a133");

        d = new Sha1Digest();
        d.update(v4);
        assertThat(d).hasToString("sha1:855426068ee8939df6bce2c2c4b1e7346532a133");
    }
}