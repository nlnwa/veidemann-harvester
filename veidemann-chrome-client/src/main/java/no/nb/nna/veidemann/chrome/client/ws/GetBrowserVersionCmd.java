package no.nb.nna.veidemann.chrome.client.ws;

import no.nb.nna.veidemann.chrome.client.ws.GetBrowserVersionCmd.Response;

public class GetBrowserVersionCmd extends Command<Response> {
    public GetBrowserVersionCmd(Cdp client) {
        super(client, "Browser", "getVersion", Response.class);
    }

    public static class Response {
        private String protocolVersion;
        private String product;
        private String revision;
        private String userAgent;
        private String jsVersion;

        /**
         * Protocol version.
         */
        public String protocolVersion() {
            return protocolVersion;
        }

        /**
         * Product name.
         */
        public String product() {
            return product;
        }

        /**
         * Product revision.
         */
        public String revision() {
            return revision;
        }

        /**
         * User-Agent.
         */
        public String userAgent() {
            return userAgent;
        }

        /**
         * V8 version.
         */
        public String jsVersion() {
            return jsVersion;
        }

        public String toString() {
            return "Version{protocolVersion=" + protocolVersion + ", product=" + product + ", revision=" + revision + ", userAgent=" + userAgent + ", jsVersion=" + jsVersion + "}";
        }
    }
}
