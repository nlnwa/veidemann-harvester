package no.nb.nna.broprox.chrome.client.ws;

public final class CdpError {

    final long code;

    final String message;

    public CdpError(long code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String toString() {
        return "{" + "code=" + code + ", message=\"" + message + "\"}";
    }

}
