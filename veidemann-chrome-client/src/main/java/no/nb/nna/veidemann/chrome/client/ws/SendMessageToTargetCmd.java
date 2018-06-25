package no.nb.nna.veidemann.chrome.client.ws;

public class SendMessageToTargetCmd extends Command<Void> {
    protected SendMessageToTargetCmd(Cdp client, String sessionId, Command wrappedCommand) {
        super(client, "Target", "sendMessageToTarget", Void.TYPE);
        withParam("sessionId", sessionId);
        withParam("message", wrappedCommand.serialize());
    }
}
