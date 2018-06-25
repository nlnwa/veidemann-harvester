package no.nb.nna.veidemann.chrome.client.ws;

import no.nb.nna.veidemann.chrome.client.ws.GetBrowserContextsCmd.Response;

import java.util.List;

public class GetBrowserContextsCmd extends Command<Response> {
    public GetBrowserContextsCmd(Cdp client) {
        super(client, "Target", "getBrowserContexts", Response.class);
    }

    public static class Response {
        private List<String> browserContextIds;
        public List<String> browserContextIds() {
            return browserContextIds;
        }
    }
}
