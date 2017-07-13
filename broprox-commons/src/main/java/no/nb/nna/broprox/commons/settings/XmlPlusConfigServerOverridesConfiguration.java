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
package no.nb.nna.broprox.commons.settings;

import java.io.IOException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import no.nb.nna.broprox.model.ConfigProto.LogLevels;
import no.nb.nna.broprox.model.ConfigProto.LogLevels.LogLevel;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationListener;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Reconfigurable;

/**
 * A log4j configuration implementation which reads an ordinary xml configuration and then adds loggers from a config
 * server.
 */
public class XmlPlusConfigServerOverridesConfiguration extends XmlConfiguration {

    private ScheduledFuture<?> future;

    int intervalSeconds;

    public XmlPlusConfigServerOverridesConfiguration(final LoggerContext loggerContext,
            final ConfigurationSource configSource) {
        super(loggerContext, configSource);
    }

    @Override
    protected void doConfigure() {
        super.doConfigure();

        try {
            ConfigServer configServer = ConfigServerFactory.getConfigServer();
            intervalSeconds = configServer.getConfigReloadInterval();

            LogLevels logLevels = configServer.getLogLevels();
            logLevels.getLogLevelList().forEach(l -> configureLogger(l));
        } catch (Exception ex) {
            LOGGER.error("Could not read config overrides from config server", ex);
        }

        if (intervalSeconds > 0) {
            try {
                future = getScheduler()
                        .scheduleWithFixedDelay(new WatchRunnable(), intervalSeconds, intervalSeconds,
                                TimeUnit.SECONDS);
            } catch (Exception ex) {
                LOGGER.error(ex.getMessage(), ex);
            }
        }
    }

    void configureLogger(LogLevel logLevel) {
        String loggerName = logLevel.getLogger();
        Level level = Level.getLevel(logLevel.getLevel().name());

        LOGGER.debug("Setting {} to {}", loggerName, level);

        LoggerConfig loggerConfig = getLogger(loggerName);
        if (loggerConfig == null) {
            AppenderRef[] refs = new AppenderRef[0];
            loggerConfig = LoggerConfig.createLogger(false, level, loggerName, null, refs, null, this, null);
            addLogger(loggerName, loggerConfig);
        } else {
            loggerConfig.setLevel(level);
        }
    }

    @Override
    public Configuration reconfigure() {
        try {
            final ConfigurationSource source = getConfigurationSource().resetInputStream();
            if (source == null) {
                return null;
            }
            LOGGER.info("Reloading configuration");
            final XmlPlusConfigServerOverridesConfiguration config
                    = new XmlPlusConfigServerOverridesConfiguration(getLoggerContext(), source);
            return config;
        } catch (final IOException ex) {
            LOGGER.error("Cannot locate file {}", getConfigurationSource(), ex);
        }
        return null;
    }

    @Override
    public boolean stop(long timeout, TimeUnit timeUnit) {
        stop(future);
        super.stop(timeout, timeUnit);
        getScheduler().decrementScheduledItems();
        return true;
    }

    @Override
    public void start() {
        getScheduler().incrementScheduledItems();
        super.start();
    }

    private class WatchRunnable implements Runnable {

        @Override
        public void run() {
            for (final ConfigurationListener configurationListener : listeners) {
                LoggerContext.getContext(false)
                        .submitDaemon(new ReconfigurationRunnable(configurationListener,
                                XmlPlusConfigServerOverridesConfiguration.this));
            }
        }

    }

    /**
     * Helper class for triggering a reconfiguration in a background thread.
     */
    private static class ReconfigurationRunnable implements Runnable {

        private final ConfigurationListener configurationListener;

        private final Reconfigurable reconfigurable;

        public ReconfigurationRunnable(final ConfigurationListener configurationListener,
                final Reconfigurable reconfigurable) {
            this.configurationListener = configurationListener;
            this.reconfigurable = reconfigurable;
        }

        @Override
        public void run() {
            configurationListener.onChange(reconfigurable);
        }

    }
}
