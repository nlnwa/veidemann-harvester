parser grammar RobotstxtParser;

options {
    language = Java;
    tokenVocab=RobotstxtLexer;
}

@header {
  package no.nb.nna.veidemann.robots;
}

robotstxt : entries* EOF;

entries : entry+;

entry : ( ( startgroupline+
  (groupmemberline | nongroupline | comment)*
  | nongroupline
  | commentline) );

startgroupline : LWS? USER_AGENT LWS? COLON LWS? agentvalue comment? EOL+;

groupmemberline : LWS? (pathmemberfield | othermemberfield) comment? EOL+;

nongroupline : LWS? (urlnongroupfield | othernongroupfield) comment? EOL+;

commentline : comment EOL+;

pathmemberfield : pathmembertype LWS? COLON LWS? pathvalue;
othermemberfield : othermembertype LWS? COLON LWS? textvalue;
urlnongroupfield : urlnongrouptype LWS? COLON LWS? urlvalue;
othernongroupfield : othernongrouptype LWS? COLON LWS? textvalue;

comment : LWS? COMMENT;
agentvalue : textvalue;
pathmembertype : DISALLOW | ALLOW;
othermembertype : textvalue;
othernongrouptype : textvalue;
pathvalue : SLASH textvalue (SLASH textvalue)*;
urlvalue : (VALUECHAR | COLON | SLASH)*;
textvalue : (VALUECHAR | SP )*;
urlnongrouptype : SITEMAP;
