package no.nb.nna.broprox.chrome.client.ws;

public class CdpException extends Exception {

    public CdpException(long code, String message) {
        super(message + " (" + code + ")");
    }
}
