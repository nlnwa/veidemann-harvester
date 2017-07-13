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

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.plugins.Plugin;

/**
 * A log4j configuration factory for creating {@link XmlPlusConfigServerOverridesConfiguration}.
 */
@Plugin(name = "XmlPlusConfigServerOverridesConfigurationFactory", category = "ConfigurationFactory")
@Order(10)
public class XmlPlusConfigServerOverridesConfigurationFactory extends ConfigurationFactory {

    /**
     * Valid file extensions for XML files.
     */
    public static final String[] SUFFIXES = new String[]{".xml", "*"};

    /**
     * Returns the file suffixes for XML files.
     *
     * @return An array of File extensions.
     */
    @Override
    public String[] getSupportedTypes() {
        return SUFFIXES;
    }

    @Override
    public Configuration getConfiguration(LoggerContext loggerContext, ConfigurationSource source) {
        return new XmlPlusConfigServerOverridesConfiguration(loggerContext, source);
    }

}
