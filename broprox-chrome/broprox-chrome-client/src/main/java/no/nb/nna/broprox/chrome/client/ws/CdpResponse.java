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
package no.nb.nna.broprox.chrome.client.ws;

import com.google.gson.JsonElement;

public final class CdpResponse {

    final long id;

    final JsonElement result;

    final CdpError error;

    final String method;

    final JsonElement params;

    public CdpResponse(long id, JsonElement result, CdpError error, String method, JsonElement params) {
        this.id = id;
        this.result = result;
        this.error = error;
        this.method = method;
        this.params = params;
    }

    @Override
    public String toString() {
        return "CdpResponse{" + "id=" + id + ", result=" + result + ", error=" + error + ", method=" + method + ", params=" + params + '}';
    }

}
