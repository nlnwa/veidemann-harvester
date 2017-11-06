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

/**
 *
 */
public enum ResourceType {
    XHR("xhr", "XHR", Category.XHR, true),
    Fetch("fetch", "Fetch", Category.XHR, true),
    EventSource("eventsource", "EventSource", Category.XHR, true),
    Script("script", "Script", Category.Script, true),
    Snippet("snippet", "Snippet", Category.Script, true),
    Stylesheet("stylesheet", "Stylesheet", Category.Stylesheet, true),
    Image("image", "Image", Category.Image, false),
    Media("media", "Media", Category.Media, false),
    Font("font", "Font", Category.Font, false),
    Document("document", "Document", Category.Document, true),
    TextTrack("texttrack", "TextTrack", Category.Other, true),
    WebSocket("websocket", "WebSocket", Category.WebSocket, false),
    Other("other", "Other", Category.Other, false),
    SourceMapScript("sm-script", "Script", Category.Script, false),
    SourceMapStyleSheet("sm-stylesheet", "Stylesheet", Category.Stylesheet, false),
    Manifest("manifest", "Manifest", Category.Manifest, true);

    final String name;

    final String title;

    final Category category;

    final boolean isTextType;

    private ResourceType(String name, String title, Category category, boolean isTextType) {
        this.name = name;
        this.title = title;
        this.category = category;
        this.isTextType = isTextType;
    }

    public static ResourceType forName(String name) {
        try {
            return valueOf(name);
        } catch (IllegalArgumentException e) {
            return Other;
        }
    }

    public enum Category {
        XHR("XHR and Fetch", "XHR"),
        Script("Scripts", "JS"),
        Stylesheet("Stylesheets", "CSS"),
        Image("Images", "Img"),
        Media("Media", "Media"),
        Font("Fonts", "Font"),
        Document("Documents", "Doc"),
        WebSocket("WebSockets", "WS"),
        Manifest("Manifest", "Manifest"),
        Other("Other", "Other");

        final String title;

        final String shortTitle;

        private Category(String title, String shortTitle) {
            this.title = title;
            this.shortTitle = shortTitle;
        }

    }
}
