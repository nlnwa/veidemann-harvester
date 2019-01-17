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

package no.nb.nna.veidemann.commons.util;

import com.google.protobuf.Message;
import no.nb.nna.veidemann.api.ConfigProto.Seed;
import no.nb.nna.veidemann.api.ControllerProto.SeedListReply;
import no.nb.nna.veidemann.api.ControllerProto.SeedListRequest;
import no.nb.nna.veidemann.api.config.v1.Label;
import no.nb.nna.veidemann.api.config.v1.Meta;
import no.nb.nna.veidemann.commons.util.ApiTools.ListReplyWalker.CheckedFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static no.nb.nna.veidemann.commons.util.ApiTools.ListReplyWalker;
import static no.nb.nna.veidemann.commons.util.ApiTools.buildLabel;
import static no.nb.nna.veidemann.commons.util.ApiTools.buildMeta;
import static org.assertj.core.api.Assertions.assertThat;

/**
 *
 */
public class ApiToolsTest {
    @Test
    public void testGetFirstLabelWithKey() {
        Meta meta = buildMeta("name", "descr",
                buildLabel("aa", "bb"), buildLabel("cc", "dd"), buildLabel("aa", "ee"));

        assertThat(ApiTools.getFirstLabelWithKey(meta, "aa")).isPresent().contains(buildLabel("aa", "bb"));
        assertThat(ApiTools.getFirstLabelWithKey(meta, "cc")).isPresent().contains(buildLabel("cc", "dd"));
        assertThat(ApiTools.getFirstLabelWithKey(meta, "bb")).isNotPresent();
    }

    /**
     * Test of hasLabel method, of class ApiTools.
     */
    @Test
    public void testHasLabel() {
        Meta meta = buildMeta("name", "descr", buildLabel("aa", "bb"), buildLabel("cc", "dd"));

        Label labelToFind1 = buildLabel("aa", "bb");
        Label labelToFind2 = buildLabel("cc", "dd");
        Label labelToFind3 = buildLabel("ee", "ff");

        assertThat(ApiTools.hasLabel(meta, labelToFind1)).isTrue();

        assertThat(ApiTools.hasLabel(meta, labelToFind1, labelToFind2)).isTrue();

        assertThat(ApiTools.hasLabel(meta, labelToFind2, labelToFind1)).isTrue();

        assertThat(ApiTools.hasLabel(meta, labelToFind3)).isFalse();

        assertThat(ApiTools.hasLabel(meta, labelToFind3, labelToFind1)).isFalse();

        assertThat(ApiTools.hasLabel(meta)).isFalse();
    }

    @Test
    public void testListReplyWalker() {
        SeedListRequest.Builder request = SeedListRequest.newBuilder().setName("foo").setPageSize(5);

        CheckedFunction<SeedListRequest, Message> fetchFunc = r -> {
            SeedListReply reply;
            switch (r.getPage()) {
                case 0:
                    reply = SeedListReply.newBuilder()
                            .addValue(Seed.newBuilder().setId("id1"))
                            .addValue(Seed.newBuilder().setId("id2"))
                            .addValue(Seed.newBuilder().setId("id3"))
                            .build();
                    break;
                case 1:
                    reply = SeedListReply.newBuilder()
                            .addValue(Seed.newBuilder().setId("id4"))
                            .addValue(Seed.newBuilder().setId("id5"))
                            .build();
                    break;
                default:
                    reply = SeedListReply.getDefaultInstance();
            }
            return reply;
        };

        List<String> values = new ArrayList<>();
        ListReplyWalker<SeedListRequest, Seed> walker = new ListReplyWalker<>();

        // Test with pageSize = 5
        walker.walk(request,
                fetchFunc,
                v -> values.add(v.getId()));

        assertThat(values).containsExactly("id1", "id2", "id3", "id4", "id5");
        assertThat(request.getPageSize()).isEqualTo(5);

        // Test with default pageSize (100)
        request.setPageSize(0);
        walker.walk(request,
                fetchFunc,
                v -> values.add(v.getId()));

        assertThat(values).containsExactly("id1", "id2", "id3", "id4", "id5", "id1", "id2", "id3", "id4", "id5");
        assertThat(request.getPageSize()).isEqualTo(100);
    }
}