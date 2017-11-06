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
package no.nb.nna.veidemann.contentwriter.warc;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class WarcWriterPool implements AutoCloseable {

    private final File targetDir;

    private final long maxFileSize;

    private final boolean compress;

    private LinkedBlockingDeque<PooledWarcWriter> pool;

    private final AtomicBoolean closed = new AtomicBoolean(true);

    private final AtomicInteger poolSize;

    /**
     * Creates the pool.
     * <p>
     * @param poolSize maximum number of writers residing in the pool
     */
    public WarcWriterPool(final File targetDir, final long maxFileSize, final boolean compress, final int poolSize) {
        this.targetDir = targetDir;
        this.maxFileSize = maxFileSize;
        this.compress = compress;
        this.poolSize = new AtomicInteger(poolSize);

        initialize();
    }

    /**
     * Gets the next free object from the pool.
     * <p>
     * @return T borrowed object
     */
    public PooledWarcWriter borrow() throws InterruptedException {
        synchronized (closed) {
            if (closed.get()) {
                throw new IllegalStateException("The WarcWriter pool is closed");
            }
            return pool.takeFirst();
        }
    }

    public PooledWarcWriter borrow(long timeout, TimeUnit unit) throws InterruptedException {
        synchronized (closed) {
            if (closed.get()) {
                throw new IllegalStateException("The WarcWriter pool is closed");
            }
            return pool.pollFirst(timeout, unit);
        }
    }

    /**
     * Returns object back to the pool.
     * <p>
     * @param object object to be returned
     */
    public void release(PooledWarcWriter object) {
        if (object != null) {
            pool.addLast(object);
        }
    }

    public final void initialize() {
        synchronized (closed) {
            if (closed.get()) {
                targetDir.mkdirs();

                pool = new LinkedBlockingDeque<>();

                for (int i = 0; i < poolSize.get(); i++) {
                    pool.add(new PooledWarcWriter(new SingleWarcWriter(targetDir, maxFileSize, compress, i)));
                }
                closed.set(false);
            } else {
                throw new IllegalStateException("Can't initialize an open WarcWriterPool");
            }
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        synchronized (closed) {
            if (closed.compareAndSet(false, true)) {
                int s = poolSize.get();
                for (int i = 0; i < s; i++) {
                    try {
                        pool.takeFirst().close();
                    } catch (Exception ex) {
                        // Can't do anything but close the others
                    }
                }
            }
        }
    }

    /**
     * Closes WARC files and opens new ones.
     *
     * @throws Exception
     */
    public void restart(boolean delete) throws Exception {
        synchronized (closed) {
            close();
            if (delete) {
                Arrays.stream(getTargetDir().listFiles()).forEach(f -> f.delete());
            }
            initialize();
        }
    }

    public File getTargetDir() {
        return targetDir;
    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public boolean isCompress() {
        return compress;
    }

    public class PooledWarcWriter implements AutoCloseable {

        final SingleWarcWriter warcWriter;

        public PooledWarcWriter(SingleWarcWriter warcWriter) {
            this.warcWriter = warcWriter;
        }

        public SingleWarcWriter getWarcWriter() {
            return warcWriter;
        }

        @Override
        public void close() throws Exception {
            release(this);
        }

    }
}
