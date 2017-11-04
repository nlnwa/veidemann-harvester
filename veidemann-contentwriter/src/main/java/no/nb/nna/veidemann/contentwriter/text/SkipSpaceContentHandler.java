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
package no.nb.nna.veidemann.contentwriter.text;

import java.util.regex.Pattern;

import no.nb.nna.veidemann.api.MessagesProto.ExtractedText;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.SAXException;

/**
 *
 */
public class SkipSpaceContentHandler extends ContentHandlerDecorator {

    // Horizontal and vertical whitespace characters
    private static final Pattern SKIP_SPACE_PATTERN = Pattern.compile("[\\h\\v]+");

    private static final Pattern SENTENCE_PATTERN = Pattern.compile("[.:!?]+");

    private static final Pattern WORD_PATTERN = Pattern.compile("[^\\p{IsLatin}]+");

//    private static final LanguageDetect langDetector = new LanguageDetect();
    private final Metadata metadata;

    private ExtractedText.Builder extractedText = ExtractedText.newBuilder();

    private StringBuilder stringBuilder;

    private String text;

    private long sentenceCount = 0;

    private long wordCount = 0;

    private long longWordCount = 0;

    private long characterCount = 0;

    public SkipSpaceContentHandler(final Metadata metadata) {
        super(new WriteOutContentHandler(-1));
        this.metadata = metadata;
        this.stringBuilder = new StringBuilder();
    }

    @Override
    public void endElement(String uri, String localName, String name) throws SAXException {
        super.endElement(uri, localName, name);
        stringBuilder.append(" ");
    }

    @Override
    public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        stringBuilder.append(ch, start, length);
        super.characters(ch, start, length);
    }

    @Override
    public void endDocument() throws SAXException {
        super.endDocument();

        text = SKIP_SPACE_PATTERN.matcher(stringBuilder).replaceAll(" ").trim();
        stringBuilder = null;
        if (!text.isEmpty()) {
//            String language = langDetector.detect(text).or("n/a");
//            metadata.add("Language", language);

            SENTENCE_PATTERN.splitAsStream(text).forEach(s -> {
                sentenceCount++;
                WORD_PATTERN.splitAsStream(s).forEach(w -> {
                    wordCount++;
                    characterCount += w.length();
                    if (w.length() > 6) {
                        longWordCount++;
                    }
                });
            });
            extractedText = ExtractedText.newBuilder()
                    .setWarcId(metadata.get("warc-id"))
                    .setText(text)
                    .setSentenceCount(sentenceCount)
                    .setWordCount(wordCount)
                    .setLongWordCount(longWordCount)
                    .setCharacterCount(characterCount)
                    .setLix(calculateLix());
        }
    }

    public ExtractedText getExtractedText() {
        return extractedText.build();
    }

    public long calculateLix() {
        if (sentenceCount <= 0 || wordCount <= 0) {
            return -1L;
        }

        return (wordCount / sentenceCount) + ((longWordCount * 100) / wordCount);
    }

}
