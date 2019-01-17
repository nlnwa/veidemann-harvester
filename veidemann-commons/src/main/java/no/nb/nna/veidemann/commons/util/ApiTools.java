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

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessageV3.Builder;
import com.google.protobuf.Message;
import no.nb.nna.veidemann.api.config.v1.ConfigObject;
import no.nb.nna.veidemann.api.config.v1.ConfigRef;
import no.nb.nna.veidemann.api.config.v1.Label;
import no.nb.nna.veidemann.api.config.v1.Meta;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 *
 */
public class ApiTools {

    private ApiTools() {
    }

    /**
     * Find the first label with a specific key.
     * <p>
     * Returns an Optional describing the first label with the specified key,
     * or an empty Optional if there is no label with the specified key.
     *
     * @param meta the meta object which might contain the requested label
     * @param key  the label key
     * @return an Optional describing the first label with the specified key,
     * or an empty Optional if there is no label with the specified key
     */
    public static Optional<Label> getFirstLabelWithKey(Meta meta, String key) {
        return meta.getLabelList().stream().filter(l -> l.getKey().equals(key)).findFirst();
    }

    /**
     * Check if a meta object contains all of the submitted labels.
     *
     * @param meta        the object to check
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

    public static String createSha1Digest(String val) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(val.getBytes());
            return new BigInteger(1, md.digest()).toString(16);
        } catch (NoSuchAlgorithmException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Helper class for traversing all values of a paged result.
     * <p>
     * Typical usage:
     * <code>
     * ListReplyWalker<SeedListRequest, Seed> walker = new ListReplyWalker<>();
     * <p>
     * walker.walk(SeedListRequest.newBuilder().setName("foo"),
     * r -> db.listSeeds(r),
     * v -> values.add(System.out.println(v)));
     * </code>
     *
     * @param <R> request type
     * @param <V> value list element type
     */
    public static class ListReplyWalker<R extends Message, V extends Message> {

        /**
         * Walk every element from the request.
         * <p>
         * This method will handle paging and repeat the query until all elements of the result set are consumed.
         *
         * @param requestBuilder a builder for the request.
         * @param fetchFunc      a function taking a request and returning a result set
         * @param consumer       the function to be applied to all elements of the result
         */
        public void walk(Builder requestBuilder, CheckedFunction<R, ? extends Message> fetchFunc, Consumer<V> consumer) {
            FieldDescriptor pageField = requestBuilder.getDescriptorForType().findFieldByName("page");
            FieldDescriptor pageSizeField = requestBuilder.getDescriptorForType().findFieldByName("page_size");

            if (pageField == null || pageSizeField == null) {
                throw new IllegalArgumentException("The request is not a paging list request");
            }

            if (requestBuilder.getField(pageSizeField).equals(0)) {
                requestBuilder.setField(pageSizeField, 100);
            }

            int page = 0;

            R request = (R) requestBuilder.setField(pageField, page).build();

            Message resultSet = null;
            try {
                resultSet = fetchFunc.apply(request);
            } catch (Exception e) {
                throw new RuntimeException("Fetch func failed");
            }
            FieldDescriptor resultValuesField = resultSet.getDescriptorForType().findFieldByName("value");

            if (resultValuesField == null || !resultValuesField.isRepeated()) {
                throw new IllegalArgumentException("Fetch func returned a response which is not a value list");
            }

            List<V> resultValues = (List<V>) resultSet.getField(resultValuesField);

            while (!resultValues.isEmpty()) {
                for (V obj : resultValues) {
                    consumer.accept(obj);
                }
                request = (R) requestBuilder.setField(pageField, ++page).build();
                try {
                    resultSet = fetchFunc.apply(request);
                } catch (Exception e) {
                    throw new RuntimeException("Fetch func failed");
                }
                resultValues = (List) resultSet.getField(resultValuesField);
            }
        }

        @FunctionalInterface
        public interface CheckedFunction<T, R> {
            R apply(T t) throws Exception;
        }
    }

    /**
     * Helper method for creating a ConfigRef for a ConfigObject.
     *
     * @param configObject the object to create a reference for
     * @return the created reference
     */
    public static ConfigRef refForConfig(ConfigObject configObject) {
        return ConfigRef.newBuilder().setKind(configObject.getKind()).setId(configObject.getId()).build();
    }
}
