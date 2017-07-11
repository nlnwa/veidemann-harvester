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
package no.nb.nna.broprox.commons.util;

import java.util.Arrays;

import no.nb.nna.broprox.model.ConfigProto.Label;
import no.nb.nna.broprox.model.ConfigProto.Meta;
import no.nb.nna.broprox.model.ConfigProto.Selector;

/**
 *
 */
public class ApiTools {

    private ApiTools() {
    }

    /**
     * Check if a meta object contains all of the submitted labels.
     *
     * @param meta the object to check
     * @param labelToFind one or more labels to search for
     * @return true if all labels where found
     */
    public static boolean hasLabel(Meta meta, Label... labelToFind) {
        boolean found = false;
        for (Label ltf : labelToFind) {
            found = false;
            for (Label ml : meta.getLabelList()) {
                if (ltf.equals(ml)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                break;
            }
        }
        return found;
    }

    public static Meta buildMeta(String name, String description, Label... label) {
        return Meta.newBuilder()
                .setName(name)
                .setDescription(name)
                .addAllLabel(Arrays.asList(label))
                .build();
    }

    public static Label buildLabel(String key, String value) {
        return Label.newBuilder()
                .setKey(key)
                .setValue(value)
                .build();
    }

    public static Selector buildSelector(Label... label) {
        return Selector.newBuilder()
                .addAllLabel(Arrays.asList(label))
                .build();
    }

}
