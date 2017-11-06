lexer grammar RobotstxtLexer;

options {
    language = Java;
}

@header {
  package no.nb.nna.veidemann.robots;
}

LWS : ((CR? LF)?(SP|HT)+) -> skip;
COLON : ':';
SLASH : '/';
USER_AGENT : U S E R '-' A G E N T;
ALLOW : A L L O W;
DISALLOW : D I S A L L O W;
SITEMAP : S I T E M A P;

EOL : (CR | LF | CRLF);
CR : '\r' -> skip;
LF : '\n' -> skip;
CRLF : CR LF;
SP : ' '; //<US-ASCII SP, space (32)
HT : '\t'; //<US-ASCII HT, horizontal-tab (9)>

//<any UTF-8 character except ("#", US-ASCII control characters (octets 0 - 31) and DEL (127))>;
VALUECHAR : (~(' '|'#'|'\u0000'..'\u001F'|'\u007F'|':'|'/'))+;

COMMENT : '#' ~('\u0000'..'\u001F'|'\u007F')*;

fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');
