package no.nb.nna.veidemann.chrome.client.ws;

import no.nb.nna.veidemann.chrome.client.ws.AttachToTargetCmd.Response;

public class AttachToTargetCmd extends Command<Response> {
    protected AttachToTargetCmd(Cdp client, String targetId) {
        super(client, "Target", "attachToTarget", Response.class);
        withParam("targetId", targetId);
    }

    public static class Response {
        private String sessionId;

        /**
         * Id assigned to the session.
         */
        public String sessionId() {
            return sessionId;
        }
    }
}
