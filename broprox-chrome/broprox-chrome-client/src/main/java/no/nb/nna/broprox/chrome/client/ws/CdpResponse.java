package no.nb.nna.broprox.chrome.client.ws;

import com.google.gson.JsonElement;

public final class CdpResponse {

    final long id;

    final JsonElement result;

    final CdpError error;

    final String method;

    final JsonElement params;

    public CdpResponse(long id, JsonElement result, CdpError error, String method, JsonElement params) {
        this.id = id;
        this.result = result;
        this.error = error;
        this.method = method;
        this.params = params;
    }

    @Override
    public String toString() {
        return "CdpResponse{" + "id=" + id + ", result=" + result + ", error=" + error + ", method=" + method + ", params=" + params + '}';
    }

}
