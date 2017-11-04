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
package no.nb.nna.veidemann.robotsparser;

import no.nb.nna.veidemann.robots.UseragentBaseListener;
import no.nb.nna.veidemann.robots.UseragentLexer;
import no.nb.nna.veidemann.robots.UseragentParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 *
 */
public class UserAgentParser {

    public UserAgentParser() {
    }

    public String parse(String userAgent) {
        TokenSource tokenSource = new UseragentLexer(CharStreams
                .fromString(userAgent));
        TokenStream tokens = new CommonTokenStream(tokenSource);
        UseragentParser parser = new UseragentParser(tokens);
        ParseTree p = parser.prog();
        ParseTreeWalker walker = new ParseTreeWalker();
        UserAgentListener listener = new UserAgentListener();
        walker.walk(listener, p);
        return listener.getBaseUserAgent();
    }

    private class UserAgentListener extends UseragentBaseListener {

        String baseUserAgent;

        public UserAgentListener() {
        }

        public String getBaseUserAgent() {
            return baseUserAgent;
        }

        @Override
        public void enterName(UseragentParser.NameContext ctx) {
            baseUserAgent = ctx.getText().toLowerCase();
        }

    }

}
