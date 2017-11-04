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
package no.nb.nna.veidemann.harvester.browsercontroller;

import java.util.Optional;

/**
 *
 */
public enum MimeTypes {
    TEXT_HTML("text/html", ResourceType.Document),
    TEXT_XML("text/xml", ResourceType.Document),
    TEXT_PLAIN("text/plain", ResourceType.Document),
    APPLICATION_XHTML_XML("application/xhtml+xml", ResourceType.Document),
    IMAGE_SVG_XML("image/svg+xml", ResourceType.Document),
    TEXT_CSS("text/css", ResourceType.Stylesheet),
    TEXT_XSL("text/xsl", ResourceType.Stylesheet),
    TEXT_VTT("text/vtt", ResourceType.TextTrack);

    final String mimeType;

    final ResourceType resourceType;

    private MimeTypes(String mimeType, ResourceType resourceType) {
        this.mimeType = mimeType;
        this.resourceType = resourceType;
    }

    public static Optional<MimeTypes> forType(String mimeType) {
        for (MimeTypes m : values()) {
            if (m.mimeType.equals(mimeType)) {
                return Optional.of(m);
            }
        }
        return Optional.empty();
    }

}
