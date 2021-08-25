package io.deephaven.lang.completion;

import io.deephaven.lang.generated.Token;
import io.deephaven.web.shared.fu.MappedIterable;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 */
public class AllTokenIterableImpl implements MappedIterable<Token> {

    private final Token start;

    public AllTokenIterableImpl(Token start) {
        this.start = start;
    }

    @NotNull
    @Override
    public Iterator<Token> iterator() {
        return new Iterator<Token>() {
            Token next, cur = next = start;
            Token mark;

            @Override
            public boolean hasNext() {
                computeNext();
                return next != null;
            }

            private void computeNext() {
                if (next == null) {
                    // find the next token
                    if (cur.next == null) {
                        // if the current token is empty, check if we have a mark to return to.
                        if (mark != null) {
                            // process the marked token next, as we've exhausted it's specialTokens
                            next = mark;
                            mark = null;
                            return;
                        }
                    } else {
                        next = cur.next;
                        if (mark == null) {
                            // not processing specialToken yet; we don't want to look at specialToken if we're already
                            // processing a chain of specialTokens...
                            if (next.specialToken != null) {
                                // ok, now we process specialToken.
                                // specialToken is weird. When you see one, if you want to iterate them in lexical
                                // order,
                                // you must follow the specialToken links until they dry up, then follow the `next`
                                // links
                                // until they are null.
                                // We mark the node where we start, then skip next ahead to the end of specialToken
                                // chain.
                                mark = next;
                                while (next.specialToken != null) {
                                    next = next.specialToken;
                                }
                                // Now, our next is the earliest specialToken,
                                // and our iterator can follow next links as normal.
                            }
                        }
                    }
                }
            }

            @Override
            public Token next() {
                computeNext();
                cur = next;
                if (cur == null) {
                    throw new NoSuchElementException();
                }
                next = null;
                return cur;
            }
        };
    }
}
