package no.nb.nna.veidemann.chrome.client.ws;

public class DetachFromTargetCmd extends Command<Void> {
    protected DetachFromTargetCmd(Cdp client, String sessionId) {
        super(client, "Target", "detachFromTarget", Void.TYPE);
        withParam("sessionId", sessionId);
    }
}
