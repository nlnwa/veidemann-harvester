package no.nb.nna.veidemann.chrome.client.ws;

import no.nb.nna.veidemann.chrome.client.ws.CreateBrowserContextCmd.Response;

public class CreateBrowserContextCmd extends Command<Response> {
    public CreateBrowserContextCmd(Cdp client) {
        super(client, "Target", "createBrowserContext", Response.class);
    }

    public static class Response {
        private String browserContextId;
        /**
         * The id of the context created.
         */
        public String browserContextId() {
            return browserContextId;
        }
    }
}
