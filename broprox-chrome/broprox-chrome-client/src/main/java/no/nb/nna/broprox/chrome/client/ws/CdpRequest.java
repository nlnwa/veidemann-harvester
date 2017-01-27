package no.nb.nna.broprox.chrome.client.ws;

import java.util.Map;

public final class CdpRequest {

    final long id;

    final String method;

    final Map<String, Object> params;

    CdpRequest(long id, String method, Map<String, Object> params) {
        this.id = id;
        this.method = method;
        this.params = params;
    }

}
