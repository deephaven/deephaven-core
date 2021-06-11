package io.deephaven.lang.generated;

import io.deephaven.lang.generated.ChunkerMixin.PeekStream.Checker;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

import static io.deephaven.lang.generated.ChunkerConstants.EOF;

/**
 * Used to extract some reusable token-checking logic from the generated parser.
 *
 * This will help rescue some somewhat complex code from our language-definition .jtt file,
 * and keep it here, where it can be more easily understood / edited.
 *
 */
public interface ChunkerMixin {

    interface CharPredicate {
        boolean match(char c);
    }

    char[] EQUAL_COMMA_OR_DASH = {'=', ',', '-'},
           COMMA_OR_DASH_OR_DOT = {',', '-', '.'},
           ANY_EXPR = {',', '-', '.', '(', ')', '+', '*', '/', '%', '!', '?', ':', '^', '[', ']', '{', '}', '$'}
    ;
    Set<Character> ALLOW_TYPE_PARAMS = set('<'),
        ALLOW_TYPE_OR_DOT = set('.', '<'),
        ALLOW_EQUALS = set('='),
        ALLOW_ASSIGN = set('=', '+', '-', '*', '/', '@', '&', '|', '^', '>', '<'),
        ALLOW_COLON = set(':'),
        ALLOW_EQUAL_COMMA_OR_DASH = set(EQUAL_COMMA_OR_DASH),
        ALLOW_COMMA_OR_DASH_OR_DOT = set(COMMA_OR_DASH_OR_DOT),
        ALLOW_PAREN = set('<', '('),
        ALLOW_ANY = set(ANY_EXPR);

    static Set<Character> set(char ... cs) {
        Set<Character> s = new HashSet<>();
        for (char c : cs) {
            s.add(c);
        }
        return s;
    }

    /**
     * A stream specialized for "peek ahead" during semantic LOOKAHEAD.
     *
     * Normally, lookahead will assign tokens as it is looking ahead,
     * but this can be problematic, as it can mis-identify and assign a token,
     * causing the real match to be missed.
     *
     * We instead use direct access to tokens and underlying streams,
     * so we can manually identify which paths to take; this can cause
     * our peekaheads to be called multiple times at the same place
     * (since we aren't assigning tokens for quick re-lookup),
     * but we can ameliorate that performance with
     * some light memoization on tokens (later).
     */
    class PeekStream {

        final static int
            /**
             *  Returned when a lookup was successful, and the char will be claimed.
             *  The next matcher, if any, will start with the next char;
             *  if this is the final matcher, the peek will be considered a success (returns true).
             */
            SUCCESS = 1,
            /**
             * Returned when a lookup did not fail, but is not yet complete.
             * The current matcher will continue to be used to match the next character.
             */
            MORE = 0,
            /**
             * Returned when a lookup was successful, but the current char should not be claimed.
             * If there are more matchers, that matcher will reuse the current character.
             * If there are no more matchers, the char is returned, and the peek is considered successful (returns true)
             */
            NEXT = 2,
            /**
             * Returned when a lookup was a categorical failure, and the lookahead should fail (returns false)
             */
            FAILURE = -1;

        interface Checker {

            Checker FAILED = c->FAILURE;

            int check(char c);

            default Checker then(Checker ... next) {
                // return a composite that first checks us, then does our next array.
                final Checker self = this;
                return new Checker() {

                    Checker checker = self;
                    int index = -1;

                    @Override
                    public int check(char c) {
                        int result = checker.check(c);
                        switch (result) {
                            case MORE:
                            case FAILURE:
                                return result;
                            case SUCCESS:
                                if (++index < next.length) {
                                    checker = next[index];
                                    return MORE;
                                } else {
                                    return SUCCESS;
                                }
                                // no need to break; we already return'd
                            case NEXT:
                                while (++index < next.length) {
                                    checker = next[index];
                                    result = checker.check(c);
                                    switch (result) {
                                        case FAILURE:
                                        case MORE:
                                            return result;
                                        case NEXT:
                                            // next will keep using this char
                                            // this is the only option that doesn't return,
                                            // so if we exhaust all sources, we will return NEXT via result var being set
                                            continue;
                                        case SUCCESS:
                                            // success will try to bump the index, or succeed immediately
                                            if (++index < next.length) {
                                                checker = next[index];
                                                return MORE;
                                            } else {
                                                return SUCCESS;
                                            }
                                    }
                                }
                        }

                        return result;
                    }
                };
            }

            default Checker maybe(boolean whitespace, char ... match) {
                class MaybeCheck implements Checker {
                    private boolean hasSucceeded;

                    @Override
                    public int check(char c) {
                        for (char m : match) {
                            if (c == m || (whitespace && Character.isWhitespace(m))) {
                                hasSucceeded = true;
                                return MORE;
                            }
                        }
                        return hasSucceeded ? SUCCESS : NEXT;
                    }

                }
                return new MaybeCheck();
            }
        }

        interface EofChecker extends Checker {
            /**
             * Ensures that an existing Checker is marked eof-friendly.
             */
            static EofChecker of(Checker check) {
                return check instanceof EofChecker ? (EofChecker) check : check::check;
            }

            /**
             * Here for an easy way to make a lambda.
             */
            static EofChecker create(EofChecker check) {
                return check;
            }
        }

        class CheckerWhitespace implements Checker {

            private final boolean required;

            public CheckerWhitespace(boolean required) {
                this.required = required;
            }

            @Override
            public int check(char c) {
                return Character.isWhitespace(c) ? MORE : required ? FAILURE : NEXT;
            }
        }

        class CheckerNewline implements Checker {

            private final boolean required;
            private boolean sawNl;

            public CheckerNewline(boolean required) {
                this.required = required;
            }

            @Override
            public int check(char c) {
                if (c == '\n') {
                    sawNl = true;
                }
                return c == '\n' ? MORE : required && !sawNl ? FAILURE : NEXT;
            }
        }

        class CheckerEmptyLine implements Checker {

            @Override
            public int check(char c) {
                return Character.isWhitespace(c) ? c == '\n' || c == '\r' ? SUCCESS : MORE : FAILURE;
            }
        }

        private final ChunkerMixin chunker;
        private final List<Checker> matchers;

        PeekStream(ChunkerMixin chunker) {
            this.chunker = chunker;
            matchers = new ArrayList<>();
        }

        public PeekStream whitespace() {
            matchers.add(ws());
            return this;
        }

        public PeekStream whitespace(boolean required) {
            matchers.add(ws(required));
            return this;
        }

        public PeekStream newline(boolean required) {
            matchers.add(nl(required));
            return this;
        }

        public PeekStream emptyLine() {
            matchers.add(new CheckerEmptyLine());
            return this;
        }

        PeekStream nextChar(Predicate<Character> matcher) {
            matchers.add(c->matcher.test(c) ? SUCCESS : FAILURE);
            return this;
        }

        public Checker ws() {
            return ws(false);
        }

        public Checker nl(boolean required) {
            return new CheckerNewline(required);
        }

        public Checker ws(boolean required) {
            return new CheckerWhitespace(required);
        }

        public PeekStream identifier(Set<Character> allowedEndings) {
            matchers.add(id(allowedEndings));
            return this;
        }

        private Checker id(Set<Character> allowedEndings) {
            class IdChecker implements Checker {
                boolean part;
                @Override
                public int check(char value) {
                    if (part) {
                        return Character.isJavaIdentifierPart(value) ? MORE :
                            Character.isWhitespace(value) || allowedEndings.contains(value) ? NEXT : FAILURE;
                    }
                    part = true;
                    return Character.isJavaIdentifierStart(value) ? MORE : FAILURE;
                }
            }
            return new IdChecker();
        }

        PeekStream typeParams(boolean required) {
            matchers.add(typeParamChecker(required));
            return this;
        }

        Checker typeParamChecker(boolean required) {
            class TypeParamChecker implements Checker {
                private int depth;
                @Override
                public int check(char value) {
                    switch (depth) {
                        case 0:
                            // when depth is zero, we must be finding an opening <
                            depth++;
                            return value == '<' ? MORE : required ? FAILURE : NEXT;
                        case 1:
                            if (value == '>') {
                                return SUCCESS;
                            }
                    }
                    switch (value) {
                        case '<':
                            depth++;
                            return MORE;
                        case '>':
                            depth--;
                            return MORE;
                        case '.':
                        case ',':
                        case '?':
                        case '&':
                            return MORE;
                        default:
                            return Character.isJavaIdentifierPart(value) || Character.isWhitespace(value) ? MORE : FAILURE;
                    }
                }
            }
            return new TypeParamChecker();
        }

        private PeekStream with(Checker checker) {
            matchers.add(checker);
            return this;
        }

        private PeekStream withAny(Checker ... checkers) {
            matchers.add(any(checkers));
            return this;
        }

        private PeekStream withTokens(String ... tokens) {
            class TokenChecker implements Checker {
                Checker[] tokenChecker;
                int best = FAILURE;
                boolean succeeded;
                @Override
                public int check(final char c) {
                    if (Character.isWhitespace(c)) {
                        tokenChecker = null;
                        return MORE;
                    }
                    if (tokenChecker == null) {
                        tokenChecker = new Checker[tokens.length];
                        for (int i = 0; i < tokens.length; i++) {
                            tokenChecker[i] = is(tokens[i]);
                        }
                    }
                    for (int i = 0; i < tokenChecker.length; i++) {
                        int result = tokenChecker[i].check(c);
                        switch (result) {
                            case FAILURE:
                                tokenChecker[i] = Checker.FAILED;
                                break;
                            case NEXT:
                                throw new IllegalStateException("NEXT is not a valid response for token checker");
                            case SUCCESS:
                                return result;
                            case MORE:
                                best = MORE;
                        }
                    }
                    return best;
                }
            }
            matchers.add(new TokenChecker());
            return this;
        }

        private Checker any(Checker ... checkers) {

            class AnyChecker implements Checker {

                int best = FAILURE;

                @Override
                public int check(char c) {
                    for (int i = 0; i < checkers.length; i++) {
                        int result = checkers[i].check(c);
                        switch (result) {
                            case FAILURE:
                                checkers[i] = Checker.FAILED;
                                break;
                            case NEXT:
                            case SUCCESS:
                                return result;
                            case MORE:
                                best = MORE;
                        }
                    }
                    return best;
                }
            }
            return new AnyChecker();
        }

        public PeekStream eofOr(char ... cs) {
            matchers.add(eof(cs));
            return this;
        }

        public PeekStream assign() {
            // Assign matchers are tricky.
            // There are valid +=, >>= variants, plain =, and then ==, =~ and ==~ non-assignment operators.
            // We need to match any valid assignment operators, but none of the binary expression operators.

            char[] firstMatch = {0};

            // Optional valid prefixes:
            // "+" | "-" | "*" | "**" | "/" | "%" | "@" | "&" | "|" | "^" | ">>" | "<<" > // Consider //=
            matchers.add(match-> {
                // we track what the first matched char is, to alter behavior in proceeding matchers
                firstMatch[0] = match;
                // First step... Check if our character is a valid, prefixed assignment, like +=
                switch (match) {
                    case '=':
                        // Keep using this = in the next check.
                        return NEXT;
                    case '+':
                    case '-':
                    case '*':
                    case '/':
                    case '%':
                    case '@':
                    case '&':
                    case '|':
                    case '^':
                    case '>':
                    case '<':
                        // A possible match; return success to consume char and proceed to next matcher
                        return SUCCESS;
                }
                // no dice
                return FAILURE;
            });

            matchers.add(match-> {
                // If the first match was =, keep passing it along.
                if (firstMatch[0] == '=') {
                    return NEXT;
                }
                // Now, we're looking for the valid double-char prefixes, **=, >>= and <<=
                switch (match) {
                    case '=':
                        // valid prefix, unless it's <= or >=
                        return firstMatch[0] == '<' || firstMatch[0] == '>' ? FAILURE : SUCCESS;
                    case '*':
                    case '>':
                    case '<':
                        if (firstMatch[0] == match) {
                            // only successful if previous char was the same char.
                            return SUCCESS;
                        }
                }
                // Anything else is failure.
                return FAILURE;
            });

            // Finally, eating the "one valid =" that we expect
            matchers.add(match ->
                match == '=' ? SUCCESS : FAILURE
            );

            // Now, make sure we aren't actually a binary operator, like == or =~
            matchers.add(match -> {
                if (Character.isWhitespace(match)) {
                    // whitespace is great, definitely not a binary operator ( `= =` is illegal)
                    return NEXT;
                }
                switch (match) {
                    case '=': // ==
                    case '~': // =~
                        // Binary operators... not assignments.
                        return FAILURE;
                }
                // Anything else is fine.  We don't want to guess at "any possible valid expression character", ever.
                return NEXT;
            });
            return this;
        }

        public EofChecker eof(char ... cs) {
            return match->{
                if (match == 0) { return SUCCESS; } // EOF
                for (char c : cs) {
                    if (match == c) {
                        return SUCCESS;
                    }
                }
                return FAILURE;
            };
        }

        public PeekStream exact(char ... cs) {
            matchers.add(is(cs));
            return this;
        }

        public PeekStream exact(String cs) {
            matchers.add(is(cs));
            return this;
        }
        public Checker is(char ... cs) {
            return match->{
                for (char c : cs) {
                    if (match == c) {
                        return SUCCESS;
                    }
                }
                return FAILURE;
            };
        }

        public Checker is(String s) {
            class IsChecker implements Checker {
                private int pntr;
                @Override
                public int check(char c) {
//                    if (s.length() <= pntr){
//                        return FAILURE;
//                    }
                    if (c == s.charAt(pntr)) {
                        pntr ++;
                        return pntr >= s.length() ? SUCCESS : MORE;
                    }
                    return FAILURE;
                }
            }
            return new IsChecker();
        }

        public boolean matches(boolean peeking) {
            // hokay!  as we're matching, we'll want to first check with any tokens we might have,
            // then we'll want to do character matching on the underlying source stream
            Token tok = chunker.curToken().next;
            final int begin = tok == null ? -1 : tok.tokenBegin;
            final Iterator<Checker> itr = matchers.iterator();
            assert itr.hasNext() : "Cannot match w/out a Checker...";
            Checker checker = itr.next();
            char c = 0;
            int result;
            boolean read = true;
            boolean isEof = false, success = false;
            while (tok != null) {
                final char[] chars = tok.image.toCharArray();
                tok = tok.next;
                for (int i = 0;
                     i < chars.length;
                     ) {
                    if (read) {
                        c = chars[i++];
                    } else {
                        read = true;
                        i++;
                    }
                    result = checker.check(c);
                    switch (result) {
                        // not very interesting when reading existing tokens, just yet.
                        // in the future, we may need to also consider re-typing these tokens.
                        // if we do, make sure to set a break point where token type is set,
                        // to see if any other state machina will need to be updated (hopefully this can be avoided).
                        case NEXT:
                            // give the current char to the next checker.
                            read = false;
                            if (itr.hasNext()) {
                                i --;
                            }
                        case SUCCESS:
                            if (itr.hasNext()) {
                                checker = itr.next();
                            } else {
                                return true;
                            }
                        case MORE:
                            continue;
                        case FAILURE:
                            return false;
                        default:
                            throw new AssertionError("missing case " + result);
                    }
                }
            }
            // still haven't return; all tokens exhausted, start checking the stream!
            int backup = 0;
            try {
                loop:
                while (checker != null) {
                    if (read) {
                        try {
                            c = chunker.next();
                            backup++;
                        } catch (IOException e) {
                            if (checker instanceof EofChecker) {
                                // normal; this is a completion that expects eof.  fall through to normal handling.
                                isEof = true;
                            } else {
                                // unexpected eof.
                                // User is likely typing something, and we simply don't match enough yet to know.
                                if (checker instanceof CheckerWhitespace && !itr.hasNext()) {
                                    success = true;
                                    break;
                                }
                                return false;
                            }
                        }
                    } else {
                        read = true;
                    }
                    result = checker.check(c);
                    switch (result) {
                        case NEXT:
                            read = false;
                        case SUCCESS:
                            if (itr.hasNext()) {
                                checker = itr.next();
                            } else {
                                success = true;
                                break loop;
                            }
                        case MORE:
                            continue;
                        case FAILURE:
                            success = false;
                            return false;
                    }
                }
            } finally {
                if (peeking || !success) {
                    chunker.back(backup, begin);
                }
            }
            if (isEof && !success) {
                // TODO: user is typing something incomplete;
                // mark this location which was checked as possibly valid,
                // to be used as a context for guessing what user is doing,
                // in the hopes of offering some meaningful completion.
            }
            return success;
        }
    }

    default void report(int code, Throwable e) {
        // need to actually handle this correctly.
        e.printStackTrace();
        throw new AssertionError("implement report()", e);
    }

    char next() throws IOException;

    Token curToken();

    boolean isLookingAhead();

    default PeekStream peek() {
        return new PeekStream(this);
    }

    default boolean isTypedAssign() {
        return peek()
            .whitespace()
            .identifier(ALLOW_TYPE_PARAMS)
            .whitespace()
            .typeParams(false)
            .whitespace()
            .identifier(ALLOW_ASSIGN)
            .whitespace()
            .assign()
            .matches(true)
        ;
    }
    default boolean isAssign() {
        return peek()
            .whitespace()
            .identifier(ALLOW_ASSIGN)
            .whitespace()
            .assign()
            .matches(true)
        ;
    }

    default boolean isScope() {
        return peek()
            .whitespace()
            .identifier(ALLOW_TYPE_OR_DOT)
            .whitespace()
            .typeParams(false)
            .whitespace()
            .exact('.')
            .whitespace()
            .nextChar(Character::isJavaIdentifierStart)
            .matches(true);
    }

    default boolean isBinExprAcrossNewline() {
        final PeekStream stream = peek();
        return stream
            .whitespace()
            .withAny(
                stream.is('.', '!', '?', ':', '*', '<', '>', '=', '+', '-', '/', '%', '&', '|', '^', '~'),
                stream.is("in"),
                stream.is("as"),
                stream.is("is"),
                stream.is("instanceof")
            )
            .whitespace()
            .matches(true);
    }

    default boolean isPythonAnnotated() {
        return peek()
            .whitespace()
            .identifier(ALLOW_COLON)
            .whitespace()
            .exact(':')
            .matches(true);
    }

    default boolean isTypeParam() {
        return peek().whitespace(false).typeParams(true).matches(true);
    }

    default boolean isReturnTypeThenInvoke() {
        final PeekStream matcher = peek().whitespace();
        matcher.identifier(ALLOW_TYPE_PARAMS).whitespace();
        matcher.typeParams(false).whitespace();
        matcher.identifier(ALLOW_PAREN).whitespace();

        return matcher.eofOr('(').matches(true);
    }
    default boolean isTypedInvoke(boolean ctor) {
        final PeekStream matcher = peek()
            .whitespace();
        if (!ctor) {
            matcher.typeParams(true).whitespace();
        }
        matcher.identifier(ALLOW_PAREN).whitespace();

        if (ctor) {
            matcher.typeParams(true).whitespace();
        }
        return matcher
            .eofOr('(')
            .matches(true);
    }

    default boolean isMethodDecl() {
        final PeekStream matcher = peek();
        matcher.whitespace()
            .withTokens(
                    // blech. this is ...ugly AF. We might need to delete this, and make actual productions for GroovyMethodDecl
                "public",
                "private",
                "protected",
                "static",
                "final",
                "strictfp",
                "default"
        );
        matcher.typeParams(false).whitespace();
        matcher.identifier(ALLOW_PAREN).whitespace();

        return matcher
            .eofOr('(')
            .matches(true);
    }

    default boolean isClassDecl() {
        return peek()
            .whitespace(false)
            .exact("class")
            .whitespace()
            .matches(true);
    }

    default boolean isDef() {
        final boolean result = peek()
                .whitespace(false)
                .exact("def")
                .whitespace()
                .matches(true);
        return result;
    }

    default boolean isPythonSized(int numIndent) {
        StringBuilder expected = new StringBuilder();
        for (int i = numIndent; i-->0;) {
            expected.append(" ");
        }
        boolean result = peek()
                .newline(false)
                .exact(expected.toString())
                .matches(true);
        return result;
    }

    default boolean isEmptyLine() {
        return peek()
            .emptyLine()
            .matches(true);
    }

    default boolean isParamList() {
        final PeekStream stream = peek();
        return stream
            .whitespace()
            .with(new Checker() {
                char prev = 0;
                @Override
                public int check(char c) {
                    switch (c) {
                        case '>':
                            if (prev == '-') {
                                return PeekStream.SUCCESS;
                            }
                            // intentional fall-through
                        case '-':
                            prev = c;
                            return PeekStream.MORE;
                        case 0:
                        case '\n':
                        case '\r':
                            return PeekStream.FAILURE;
                        default:
                            prev = c;
                            return PeekStream.MORE;
                    }
                }
            })
            .matches(true);
    }

    void back(int amt, int begin);

    Token token();

    default void eatEmptyLines() {
        final PeekStream peek = peek();
        // loop while current line contains only whitespace; throw away chars iff whole line matches.
        while (true) {
            if (!peek.emptyLine().matches(false)) break;
        }
    }
    default Token eatJunk() {
        final Token token = token();
        Token root = new Token(0, "");
        root.image = "";
        // This will set root.next == token.next
        root.copyToken(token);
        root.kind = EOF;
        root.startIndex = token.endIndex;
        root.beginLine = token.endLine;
        root.beginColumn = token.endColumn;

        Token next = token;
        while (next.next != null) {
            next = next.next;
            next.kind = EOF;
        }
        token.next = root;
        char c;
        int backup = 1;
        StringBuilder b = new StringBuilder();
        loop: for(;;){
            try {
                c = next();
                switch (c) {
                    case '\n':
                    case '\r':
                        break loop;
                }
                b.append(c);
            } catch (IOException eof) {
                backup = 0;
                break;
            }
        }

        back(backup, token.tokenBegin);

        if (next == token) {
            // There were no leftover / intermediate tokens,
            // so we can just set our image onto the root token.
            root.image = b.toString();
            root.endIndex = root.startIndex + b.length();
            root.endColumn = root.endColumn + b.length();
        } else {
            // There were intermediate tokens, so we'll leave the root EOF node at HEAD,
            // then slap an extra EOF w/ leftovers from stream on the end.
            Token junk = new Token(0, "");
            assert next.next == null;
            junk.copyToken(next);
            junk.kind = EOF;
            junk.image = b.toString();
            junk.startIndex = next.endIndex;
            junk.endIndex = junk.startIndex + b.length();
            junk.beginColumn = junk.endColumn + (b.length() == 0 ? 0 : 1);
            junk.endColumn = junk.endColumn + b.length();
            junk.specialToken = null;
            next.next = junk;
        }
        return root;
    }
}
