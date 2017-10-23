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
package no.nb.nna.broprox.commons.auth;

import io.grpc.Context;

/**
 * A key for the current users e-mail address.
 *
 * Can be used to get the current users e-mail address.
 */
public class EmailContextKey {

    public static final String KEY_NAME = "veidemann.email";
    private static final Context.Key<String> key = Context.key(KEY_NAME);

    /**
     * @return the e-mail address for the current user
     */
    public static String email() {
        return key.get();
    }

    /**
     * @return the Email context key
     */
    public static Context.Key<String> getKey() {
        return key;
    }
}