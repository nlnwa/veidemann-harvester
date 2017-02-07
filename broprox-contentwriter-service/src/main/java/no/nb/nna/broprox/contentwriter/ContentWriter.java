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
package no.nb.nna.broprox.contentwriter;

import java.io.File;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import no.nb.nna.broprox.db.DbAdapter;
import no.nb.nna.broprox.db.RethinkDbAdapter;
import no.nb.nna.broprox.contentwriter.settings.Settings;
import no.nb.nna.broprox.contentwriter.text.TextExtracter;
import no.nb.nna.broprox.contentwriter.warc.WarcWriterPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for launching the service.
 */
public class ContentWriter {

    private static final Logger LOG = LoggerFactory.getLogger(ContentWriter.class);

    private static final Settings SETTINGS;

    static {
        Config config = ConfigFactory.load();
        config.checkValid(ConfigFactory.defaultReference());
        SETTINGS = ConfigBeanFactory.create(config, Settings.class);
    }

    /**
     * Create a new Broprox service.
     */
    public ContentWriter() {
    }

    /**
     * Start the service.
     * <p>
     * @return this instance
     */
    public ContentWriter start() {
        try {
            DbAdapter db = new RethinkDbAdapter(SETTINGS.getDbHost(), SETTINGS.getDbPort(), SETTINGS.getDbName());
            WarcWriterPool warcWriterPool = new WarcWriterPool(new File(SETTINGS.getWarcDir()),
                    SETTINGS.getWarcFileSize(), SETTINGS.isCompressWarc(), SETTINGS.getWarcWriterPoolSize());
            TextExtracter textExtracter = new TextExtracter();

            ApiServer apiServer = new ApiServer(db, warcWriterPool, textExtracter);

            LOG.info("Broprox content writer (v. {}) started",
                    ContentWriter.class.getPackage().getImplementationVersion());

            try {
                Thread.currentThread().join();
            } catch (InterruptedException ex) {
                // Interrupted, shut down
            }
        } catch (ConfigException ex) {
            System.err.println("Configuration error: " + ex.getLocalizedMessage());
            System.exit(1);
        } catch (Exception ex) {
            LOG.error("Could not start service", ex);
        }

        return this;
    }

    /**
     * Get the settings object.
     * <p>
     * @return the settings
     */
    public static Settings getSettings() {
        return SETTINGS;
    }

}
