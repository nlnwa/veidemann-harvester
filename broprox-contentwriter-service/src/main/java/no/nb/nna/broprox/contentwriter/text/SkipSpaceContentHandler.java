/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package no.nb.nna.broprox.contentwriter.text;

import java.util.regex.Pattern;

import no.nb.nna.broprox.db.model.CrawlLog;
import no.nb.nna.broprox.db.model.ExtractedText;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.WriteOutContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 *
 */
public class SkipSpaceContentHandler extends ContentHandlerDecorator {

    // Horizontal and vertical whitespace characters
    private static final Pattern skipSpacePattern = Pattern.compile("[\\h\\v]+");

    private static final Pattern sentencePattern = Pattern.compile("[.:!?]+");

    private static final Pattern wordPattern = Pattern.compile("[^\\p{IsLatin}]+");

//    private static final LanguageDetect langDetector = new LanguageDetect();
    private final Metadata metadata;

    private final ExtractedText extractedText;

    private StringBuilder stringBuilder;

    private String text;

    private long sentenceCount = 0;

    private long wordCount = 0;

    private long longWordCount = 0;

    private long characterCount = 0;

//    public SkipSpaceContentHandler(ContentHandler handler, Metadata metadata) {
//        super(new WriteOutContentHandler(handler, -1));
//        this.metadata = metadata;
//        this.stringBuilder = new StringBuilder();
//    }

    public SkipSpaceContentHandler(final ExtractedText extractedText, final Metadata metadata) {
        super(new WriteOutContentHandler(-1));
        this.extractedText = extractedText;
        this.metadata = metadata;
        this.stringBuilder = new StringBuilder();
    }

//    public SkipSpaceContentHandler() {
//        super(new WriteOutContentHandler(-1));
//        this.metadata = new Metadata();
//        this.stringBuilder = new StringBuilder();
//    }

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

        text = skipSpacePattern.matcher(stringBuilder).replaceAll(" ").trim();
        stringBuilder = null;
        if (!text.isEmpty()) {
//            String language = langDetector.detect(text).or("n/a");
//            metadata.add("Language", language);

            sentencePattern.splitAsStream(text).forEach(s -> {
                sentenceCount++;
                wordPattern.splitAsStream(s).forEach(w -> {
                    wordCount++;
                    characterCount += w.length();
                    if (w.length() > 6) {
                        longWordCount++;
                    }
                });
            });
            extractedText
                    .withText(text)
                    .withSentenceCount(sentenceCount)
                    .withWordCount(wordCount)
                    .withLongWordCount(longWordCount)
                    .withCharacterCount(characterCount)
                    .withLix(calculateLix());
        }
    }

//    public String getText() {
//        return text;
//    }
//
//    public long getSentenceCount() {
//        return sentenceCount;
//    }
//
//    public long getWordCount() {
//        return wordCount;
//    }
//
//    public long getLongWordCount() {
//        return longWordCount;
//    }
//
//    public long getCharacterCount() {
//        return characterCount;
//    }

    public long calculateLix() {
        if (sentenceCount <= 0 || wordCount <= 0) {
            return -1L;
        }

        return (wordCount / sentenceCount) + ((longWordCount * 100) / wordCount);
    }

}
