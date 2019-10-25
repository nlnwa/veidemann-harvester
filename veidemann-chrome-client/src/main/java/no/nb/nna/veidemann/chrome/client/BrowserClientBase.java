package no.nb.nna.veidemann.chrome.client;

import no.nb.nna.veidemann.chrome.client.BrowserClientBase.BrowserPage;
import no.nb.nna.veidemann.chrome.client.ws.CdpConnection;
import no.nb.nna.veidemann.chrome.client.ws.CdpSession;
import no.nb.nna.veidemann.chrome.client.ws.CreateBrowserContextCmd;
import no.nb.nna.veidemann.chrome.client.ws.DisposeBrowserContextCmd;
import no.nb.nna.veidemann.chrome.client.ws.GetBrowserContextsCmd;
import no.nb.nna.veidemann.chrome.client.ws.TargetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static no.nb.nna.veidemann.chrome.client.ChromeDebugProtocolBase.CLIENT_EXECUTOR_SERVICE;

public abstract class BrowserClientBase<T extends BrowserPage> implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(BrowserClientBase.class);

    ChromeDebugProtocolBase<BrowserClientBase> chromeDebugProtocol;

    CdpConnection protocolClient;

    Map<String, BrowserContext> contexts = new ConcurrentHashMap<>();

    BrowserContext defaultContext = new BrowserContext("");

    Map<String, CompletableFuture<Target>> targets = new ConcurrentHashMap<>();

    BrowserClientBase() {
    }

    public BrowserContext createIncognitoBrowserContext() throws TimeoutException, ExecutionException {
        String browserContextId = new CreateBrowserContextCmd(protocolClient).run().browserContextId();
        BrowserContext context = new BrowserContext(browserContextId);
        contexts.put(context.id, context);
        return context;
    }

    public List<BrowserContext> getBrowserContexts() {
        List<BrowserContext> result = new ArrayList<>();
        result.add(defaultContext);
        result.addAll(contexts.values());
        return result;
    }

    void init(ChromeDebugProtocolBase<BrowserClientBase> chromeDebugProtocol, ChromeDebugProtocolConfig config) {
        this.chromeDebugProtocol = chromeDebugProtocol;
        this.protocolClient = new CdpConnection(config);
        checkVersion();
        try {
            List<String> contextIds = new GetBrowserContextsCmd(protocolClient).run().browserContextIds();
            for (String contextId : contextIds) {
                contexts.put(contextId, new BrowserContext(contextId));
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    protected void checkVersion() {
        String clientVersion = getVersionNumber(version());
        String browserVersion = getVersionNumber(protocolClient.getRemoteVersion());
        if (!clientVersion.equals(browserVersion)) {
            throw new RuntimeException("Chrome client version and browser version does not match. Client: "
                    + clientVersion + ", Browser: " + browserVersion);
        }
    }

    private String getVersionNumber(String versionString) {
        String result;
        if (versionString.contains("/")) {
            result = versionString.split("/")[1];
        } else {
            result = versionString;
        }
        return result;
    }

    public abstract String version();

    public abstract boolean isClosed();

    public ChromeDebugProtocolConfig getConfig() {
        return protocolClient.getConfig();
    }

    Target getTarget(String targetId) {
        try {
            return targets.computeIfAbsent(targetId, k -> new CompletableFuture<>()).get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    boolean targetExists(String targetId) {
        return targets.containsKey(targetId) && targets.get(targetId).isDone();
    }

    CompletableFuture<Target> putTarget(Target target) {
        CompletableFuture<Target> t = targets.computeIfAbsent(target.targetInfo.targetId(), k -> new CompletableFuture<>());
        t.complete(target);
        return t;
    }

    Target removeTarget(String targetId) {
        Target target = null;
        CompletableFuture<Target> t = targets.remove(targetId);
        if (t.isDone()) {
            try {
                target = t.get();
                target.initializedCallback.complete(false);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                return null;
            }
        }
        return target;
    }

    void onTargetCreated(TargetInfo targetInfo) {
        CLIENT_EXECUTOR_SERVICE.execute(() -> {

            LOG.debug("New target: {}", targetInfo);
            if (targetExists(targetInfo.targetId())) {
                throw new IllegalStateException("Target should not exist before targetCreated");
            }

            BrowserContext context = null;
            if (targetInfo.browserContextId() != null) {
                context = contexts.get(targetInfo.browserContextId());
            }
            if (context == null) {
                context = defaultContext;
            }

            try {
                CompletableFuture<CdpSession> sessionFactory = null;
                if ("page".equals(targetInfo.type()) || "background_page".equals(targetInfo.type())) {
                    sessionFactory = protocolClient.createNewSessionClient(targetInfo);
                }
                Target target = new Target(targetInfo, sessionFactory);
                putTarget(target);

                if (target.initializedCallback.get()) {
                    LOG.trace("Target is initialized: ", targetInfo);
//            this.emit(Browser.Events.TargetCreated, target);
//            context.emit(BrowserContext.Events.TargetCreated, target);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }

        });
    }

    void onTargetDestroyed(String targetId) {
        CLIENT_EXECUTOR_SERVICE.execute(() -> {
            LOG.debug("Target destroyed: {}", targetId);
            Target target = removeTarget(targetId);
            // target.closedCallback();
            try {
                if (target != null && target.initializedCallback.get()) {
                    //        this.emit(Browser.Events.TargetDestroyed, target);
                    //        target.browserContext().emit(BrowserContext.Events.TargetDestroyed, target);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }

    void onTargetInfoChanged(TargetInfo targetInfo) {
        LOG.debug("Targetinfo changed: {}", targetInfo);
        Target target = getTarget(targetInfo.targetId());
        if (target == null) {
            throw new IllegalStateException("Target should exist before targetInfoChanged");
        }
        String previousURL = target.targetInfo.url();
        boolean wasInitialized = target.initializedCallback.isInitialized();
        target.targetInfoChanged(targetInfo);
        if (wasInitialized && !previousURL.equals(targetInfo.url())) {
//            this.emit(Browser.Events.TargetChanged, target);
//            target.browserContext().emit(BrowserContext.Events.TargetChanged, target);
        }
    }

    public T newPage(final int clientWidth, final int clientHeight) {
        return defaultContext.newPage(clientWidth, clientHeight);
    }

    /**
     * @param {string} contextId
     * @return {!Promise<!Puppeteer.Page>}
     */
    T createPageInContext(final String contextId, final int clientWidth, final int clientHeight) {
        try {
            String targetId = newTarget(contextId, clientWidth, clientHeight).get().targetId();
            Target target = getTarget(targetId);
            CompletableFuture<T> p = target.getPage();
            return p.get();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    abstract CompletableFuture<? extends CreateTargetReply> newTarget(final String contextId, final int clientWidth,
                                                                      final int clientHeight)
            throws ClientClosedException, TimeoutException, ExecutionException;

    abstract T newPageSession(final CdpSession clientSession);

    @Override
    public void close() {
        protocolClient.dispose();
        chromeDebugProtocol.clients.remove(this);
    }

    public interface BrowserPage extends AutoCloseable {
    }

    public interface CreateTargetReply {
        String targetId();
    }

    public class BrowserContext implements AutoCloseable {
        final String id;

        public BrowserContext(String id) {
            this.id = id;
        }

        T newPage(final int clientWidth, final int clientHeight) {
            return createPageInContext(id.isEmpty() ? null : id, clientWidth, clientHeight);
        }

        List<Target> getTargets() {
            return targets.values().stream()
                    .filter(t -> {
                        try {
                            return t.isDone() && t.get().targetInfo.browserContextId().equals(id);
                        } catch (Exception e) {
                            return false;
                        }
                    })
                    .map(t -> {
                        try {
                            return t.get();
                        } catch (Exception e) {
                            // Should never happen because of filter
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
        }

        boolean isIncognito() {
            return !id.isEmpty();
        }

        @Override
        public void close() throws Exception {
            if (isIncognito()) {
                try {
                    new DisposeBrowserContextCmd(protocolClient, id).run();
                } catch (Exception ex) {
                    LOG.warn("Failed closing Chrome context", ex);
                }
                contexts.remove(id);
            } else {
                throw new IllegalStateException("Non-incognito profiles cannot be closed!");
            }
        }
    }

    public class Target {
        CdpSession cdpSession;
        TargetInfo targetInfo;
        InitializedFuture initializedCallback = new InitializedFuture();
        CompletableFuture<CdpSession> sessionFactory;
        private CompletableFuture<T> page;

        public Target(TargetInfo targetInfo, CompletableFuture<CdpSession> sessionFactory) {
            this.targetInfo = targetInfo;
            this.sessionFactory = sessionFactory;
            if (!"page".equals(targetInfo.type()) || !"".equals(targetInfo.url())) {
                initializedCallback.complete(true);
            }
        }

        CompletableFuture<T> getPage() {
            if (page == null && ("page".equals(targetInfo.type()) || "background_page".equals(targetInfo.type()))) {
                page = new CompletableFuture<>();
                CLIENT_EXECUTOR_SERVICE.execute(() -> {
                    sessionFactory.thenAccept(s -> {
                        cdpSession = s;
                        page.complete(newPageSession(s));
                        initializedCallback.complete(true);
                    });
                });
            }
            return page;
        }

        void targetInfoChanged(TargetInfo targetInfo) {
            this.targetInfo = targetInfo;

            if (!initializedCallback.isInitialized()
                    && (!"page".equals(this.targetInfo.type()) || !this.targetInfo.url().isEmpty())) {
                this.initializedCallback.complete(true);
            }
        }
    }

    static class InitializedFuture extends CompletableFuture<Boolean> {
        public boolean isInitialized() {
            try {
                return isDone() && get().equals(Boolean.TRUE);
            } catch (Exception e) {
                return false;
            }
        }
    }
}
