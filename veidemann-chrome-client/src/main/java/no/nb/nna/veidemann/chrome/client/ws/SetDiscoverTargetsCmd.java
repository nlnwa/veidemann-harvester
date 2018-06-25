package no.nb.nna.veidemann.chrome.client.ws;

public class SetDiscoverTargetsCmd extends Command<Void> {
    public SetDiscoverTargetsCmd(Cdp client) {
        super(client, "Target", "setDiscoverTargets", Void.TYPE);
        withParam("discover", true);
    }
}
