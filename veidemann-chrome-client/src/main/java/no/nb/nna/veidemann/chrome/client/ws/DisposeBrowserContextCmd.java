package no.nb.nna.veidemann.chrome.client.ws;

public class DisposeBrowserContextCmd extends Command<Void> {
    public DisposeBrowserContextCmd(Cdp client, String browserContextId) {
        super(client, "Target", "disposeBrowserContext", Void.TYPE);
        withParam("browserContextId", browserContextId);
    }
}
