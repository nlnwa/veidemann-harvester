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
package no.nb.nna.veidemann.chrome.client.ws;

import com.google.gson.JsonObject;

public final class CdpResponse {

    final long id;

    final JsonObject result;

    final CdpError error;

    final String method;

    final JsonObject params;

    public CdpResponse(long id, JsonObject result, CdpError error, String method, JsonObject params) {
        this.id = id;
        this.result = result;
        this.error = error;
        this.method = method;
        this.params = params;
    }

    String serialize() {
        try {
            return Cdp.GSON.toJson(this);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(this.getClass().getSimpleName());
        String msg = serialize();

        // Restrict size of msg
        if (msg.length() > Cdp.MAX_TOSTRING_SIZE) {
            msg = msg.substring(0, Cdp.MAX_TOSTRING_SIZE) + "... ";
        }

        sb.append(msg);
        return sb.toString();
    }
}
