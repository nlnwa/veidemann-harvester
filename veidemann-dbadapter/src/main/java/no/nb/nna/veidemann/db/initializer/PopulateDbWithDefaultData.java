/*
 * Copyright 2018 National Library of Norway.
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
package no.nb.nna.veidemann.db.initializer;

import com.google.protobuf.Message;
import no.nb.nna.veidemann.api.ConfigProto.RoleMapping;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.commons.db.DbException;
import no.nb.nna.veidemann.commons.db.DbService;
import no.nb.nna.veidemann.db.ProtoUtils;
import no.nb.nna.veidemann.db.RethinkDbConfigAdapter;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PopulateDbWithDefaultData implements Runnable {
    @Override
    public void run() {
        populateDb();
    }

    private final void populateDb() {
//        ConfigAdapter db = DbService.getInstance().getConfigAdapter();
        RethinkDbConfigAdapter db = (RethinkDbConfigAdapter) DbService.getInstance().getConfigAdapter();
        try {
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("default_objects/schedule-configs.yaml")) {
                readYamlFile(in, ConfigObject.class)
                        .forEach(o -> {
                            try {
                                db.saveConfigObject(o);
                            } catch (DbException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("default_objects/politeness-configs.yaml")) {
                readYamlFile(in, ConfigObject.class)
                        .forEach(o -> {
                            try {
                                db.saveConfigObject(o);
                            } catch (DbException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("default_objects/browser-configs.yaml")) {
                readYamlFile(in, ConfigObject.class)
                        .forEach(o -> {
                            try {
                                db.saveConfigObject(o);
                            } catch (DbException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("default_objects/crawl-configs.yaml")) {
                readYamlFile(in, ConfigObject.class)
                        .forEach(o -> {
                            try {
                                db.saveConfigObject(o);
                            } catch (DbException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("default_objects/browser-scripts.yaml")) {
                readYamlFile(in, ConfigObject.class)
                        .forEach(o -> {
                            try {
                                db.saveConfigObject(o);
                            } catch (DbException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("default_objects/crawl-jobs.yaml")) {
                readYamlFile(in, ConfigObject.class)
                        .forEach(o -> {
                            try {
                                db.saveConfigObject(o);
                            } catch (DbException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            try (InputStream in = getClass().getClassLoader()
                    .getResourceAsStream("default_objects/rolemappings.yaml")) {
                readYamlFile(in, ConfigObject.class)
                        .forEach(o -> {
                            try {
                                db.saveConfigObject(o);
                            } catch (DbException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    <T extends Message> Stream<T> readYamlFile(InputStream in, Class<T> type) {
        Yaml yaml = new Yaml();
        return StreamSupport.stream(yaml.loadAll(in).spliterator(), false)
                .map(o -> ProtoUtils.rethinkToProto((Map) o, type));
    }
}
