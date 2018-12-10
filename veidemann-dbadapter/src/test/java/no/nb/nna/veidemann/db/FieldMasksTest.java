/*
 * Copyright 2018 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package no.nb.nna.veidemann.db;

import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.FieldMask;
import no.nb.nna.veidemann.api.config.v1.Kind;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FieldMasksTest {

    @Test
    public void createForFieldMaskProto() {
        FieldMask m = FieldMask.newBuilder()
                .addPaths("meta.name")
                .addPaths("meta.foo.bar")
                .addPaths("test").build();

        assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> FieldMasks.createForFieldMaskProto(m));
    }

    @Test
    public void createPluckQuery() {
        FieldMask m = FieldMask.newBuilder()
                .addPaths("meta.name")
                .addPaths("crawlConfig.extra.extractText")
                .addPaths("meta")
                .addPaths("crawlConfig.priorityWeight")
                .build();

        FieldMasks fm = FieldMasks.createForFieldMaskProto(m);
        List q = fm.createPluckQuery();

        assertThat(q).hasToString("[apiVersion, kind, id, meta, {crawlConfig=[{extra=[extractText]}, priorityWeight]}]");
    }

    @Test
    public void getValue() {
        ConfigObject.Builder co = ConfigObject.newBuilder().setApiVersion("v1").setKind(Kind.browserConfig);
        co.getBrowserConfigBuilder().setUserAgent("agent");

        FieldMask m = FieldMask.newBuilder()
                .addPaths("meta.name")
                .addPaths("apiVersion")
                .build();

        FieldMasks fm = FieldMasks.createForFieldMaskProto(m);

        assertThat(fm.getValue("apiVersion", co.build())).isEqualTo("v1");
        assertThat(fm.getValue("meta.name", co.build())).isEqualTo("");
        assertThat(fm.getValue("browserConfig.userAgent", co.build())).isEqualTo("agent");
    }
}