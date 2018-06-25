package no.nb.nna.veidemann.chrome.client.ws;

public interface TargetInfo {
    public String targetId();

    public String type();

    public String title();

    public String url();

    /**
     * Whether the target has an attached client.
     */
    public Boolean attached();

    /**
     * Opener target Id
     */
    public String openerId();

    public String browserContextId();
}
