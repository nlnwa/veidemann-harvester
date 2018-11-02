package no.nb.nna.veidemann.chrome.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ChromeDebugProtocolBase<T extends BrowserClientBase> implements AutoCloseable {
    static final ExecutorService CLIENT_EXECUTOR_SERVICE = Executors.newCachedThreadPool();
    List<T> clients = new ArrayList<>();

    T connect(ChromeDebugProtocolConfig config, T browser) {
        browser.init(this, config);
        clients.add(browser);
        return browser;
    }

    @Override
    public void close() {
        for (T c : clients) {
            try {
                c.close();
            } catch (Exception e) {
            }
        }
    }

    public int getClientCount() {
        return clients.size();
    }

    private void ensureInitialPage(BrowserClientBase browser) {
//        // Wait for initial page target to be created.
//        if (browser.targets().find(target => target.type() === 'page'))
//        return;
//
//        let initialPageCallback;
//      const initialPagePromise = new Promise(resolve => initialPageCallback = resolve);
//      const listeners = [helper.addEventListener(browser, 'targetcreated', target => {
//        if (target.type() === 'page')
//            initialPageCallback();
//      })];
//
//        await initialPagePromise;
//        helper.removeEventListeners(listeners);
    }
}
