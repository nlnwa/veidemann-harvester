/*
 * Copyright 2017 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package no.nb.nna.broprox.chrome.client.ws;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Phaser;

/**
 *
 */
public class CompleteMany extends CompletableFuture<Void> {

    final Phaser phaser = new Phaser(1);

    private Throwable exception;

    public CompleteMany(CompletableFuture<? extends Object>... futures) {
        // Register all futeres
        for (CompletableFuture<? extends Object> f : futures) {
            phaser.register();
            f.whenComplete((r, e) -> {
                if (e != null) {
                    completeExceptionally(e);
                }
                phaser.arrive();
            });
        }

        // Wait for all futures to complete in another thread
        ForkJoinPool.commonPool().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    phaser.awaitAdvanceInterruptibly(phaser.arrive());
                    complete(null);
                } catch (InterruptedException ex) {
                    completeExceptionally(ex);
                }
            }

        });
    }
}
