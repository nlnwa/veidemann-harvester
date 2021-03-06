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

public final class CdpError {

    final long code;

    final String message;

    public CdpError(long code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public String toString() {
        return "{" + "code=" + code + ", message=\"" + message + "\"}";
    }

}
