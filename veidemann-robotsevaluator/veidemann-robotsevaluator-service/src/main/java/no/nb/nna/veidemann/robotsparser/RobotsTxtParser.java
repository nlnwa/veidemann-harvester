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

import java.io.IOException;
import java.io.Reader;
import no.nb.nna.veidemann.robots.RobotstxtLexer;
import no.nb.nna.veidemann.robots.RobotstxtParser;
import no.nb.nna.veidemann.robots.RobotstxtParserBaseListener;
import no.nb.nna.veidemann.robotsparser.RobotsTxt.Directive;
import no.nb.nna.veidemann.robotsparser.RobotsTxt.DirectiveGroup;
import no.nb.nna.veidemann.robotsparser.RobotsTxt.DirectiveType;
import no.nb.nna.veidemann.robotsparser.RobotsTxt.NonGroupField;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 *
 */
public class RobotsTxtParser {

    public RobotsTxtParser() {
    }

    public RobotsTxt parse(String robotsContent) throws IOException {
        return parse(CharStreams.fromString(robotsContent));
    }

    public RobotsTxt parse(Reader robotsReader) throws IOException {
        return parse(CharStreams.fromReader(robotsReader));
    }

    public RobotsTxt parse(CharStream robotsStream) throws IOException {
        TokenSource tokenSource = new RobotstxtLexer(robotsStream);
        TokenStream tokens = new CommonTokenStream(tokenSource);
        RobotstxtParser parser = new RobotstxtParser(tokens);
        ParseTree p = parser.robotstxt();
        ParseTreeWalker walker = new ParseTreeWalker();
        RobotsTxt robotsTxt = new RobotsTxt();
        walker.walk(new RobotsListener(robotsTxt), p);
        return robotsTxt;
    }

    private class RobotsListener extends RobotstxtParserBaseListener {

        DirectiveGroup currentDirective = null;

        final RobotsTxt robotsTxt;

        public RobotsListener(RobotsTxt robotsTxt) {
            this.robotsTxt = robotsTxt;
        }

        @Override
        public void enterStartgroupline(RobotstxtParser.StartgrouplineContext ctx) {
            currentDirective.userAgents.add(ctx.agentvalue().getText().toLowerCase());
        }

        @Override
        public void enterEntry(RobotstxtParser.EntryContext ctx) {
            if (!ctx.startgroupline().isEmpty()) {
                currentDirective = new DirectiveGroup();
            }
        }

        @Override
        public void enterUrlnongroupfield(RobotstxtParser.UrlnongroupfieldContext ctx) {
            robotsTxt.otherFields.add(
                    new NonGroupField(ctx.urlnongrouptype().getText().toLowerCase(), ctx.urlvalue().getText()));
        }

        @Override
        public void enterOthernongroupfield(RobotstxtParser.OthernongroupfieldContext ctx) {
            robotsTxt.otherFields.add(
                    new NonGroupField(ctx.othernongrouptype().getText().toLowerCase(), ctx.textvalue().getText()));
        }

        @Override
        public void enterOthermemberfield(RobotstxtParser.OthermemberfieldContext ctx) {
            String fieldName = ctx.othermembertype().getText().toLowerCase();
            switch (fieldName) {
                case "cache-delay":
                    currentDirective.cacheDelay = Float.parseFloat(ctx.textvalue().getText());
                    break;
                case "crawl-delay":
                    currentDirective.crawlDelay = Float.parseFloat(ctx.textvalue().getText());
                    break;
                default:
                    currentDirective.otherFields.put(fieldName, ctx.textvalue().getText());
                    break;
            }
        }

        @Override
        public void enterPathmemberfield(RobotstxtParser.PathmemberfieldContext ctx) {
            if (ctx.pathmembertype().ALLOW() != null) {
                currentDirective.directives.add(new Directive(DirectiveType.ALLOW, ctx.pathvalue().getText()));
            }
            if (ctx.pathmembertype().DISALLOW() != null) {
                currentDirective.directives.add(new Directive(DirectiveType.DISALLOW, ctx.pathvalue().getText()));
            }
        }

        @Override
        public void exitEntry(RobotstxtParser.EntryContext ctx) {
            if (currentDirective != null) {
                robotsTxt.directives.add(currentDirective);
                currentDirective = null;
            }
        }

    }

}
