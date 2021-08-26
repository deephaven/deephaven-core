/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.string.cache;

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Locale;

/**
 * This creates an interface (with more permissive argument types, and generified return types) for
 * most public instance methods of class String.
 *
 * For static methods, often the String implementation should be used and its results converted to
 * the desired type via construction. That is, for String method M, and StringAlike class SAC,
 * follow the following pattern: new SAC(String.M(args...))
 *
 * For JavaDocs, see {@link String}.
 */
public interface StringAlike<TYPE> extends StringCompatible, Serializable {

    boolean isEmpty();

    int codePointAt(final int index);

    int codePointBefore(final int index);

    int codePointCount(final int beginIndex, final int endIndex);

    void getChars(int srcBegin, int srcEnd, char dst[], int dstBegin);

    byte[] getBytes();

    boolean contentEquals(@NotNull CharSequence cs);

    boolean equalsIgnoreCase(@NotNull CharSequence that);

    int compareToIgnoreCase(@NotNull CharSequence that);

    boolean regionMatches(boolean ignoreCase,
        int offset,
        CharSequence that,
        int thatOffset,
        int length);

    boolean startsWith(@NotNull CharSequence prefix, int offset);

    boolean startsWith(@NotNull CharSequence prefix);

    boolean endsWith(@NotNull CharSequence suffix);

    int indexOf(int ch, int fromIndex);

    int indexOf(int ch);

    int lastIndexOf(int ch, int fromIndex);

    int lastIndexOf(int ch);

    TYPE substring(int beginIndex, int endIndex);

    TYPE substring(int beginIndex);

    TYPE concat(String other);

    TYPE concat(TYPE other);

    boolean matches(CharSequence regex);

    TYPE trim();

    char[] toCharArray();

    int indexOf(CharSequence cs, int fromIndex);

    int indexOf(CharSequence cs);

    int lastIndexOf(CharSequence cs, int fromIndex);

    int lastIndexOf(CharSequence cs);

    TYPE replace(char oldChar, char newChar);

    boolean contains(CharSequence cs);

    TYPE replaceFirst(CharSequence regex, CharSequence replacement);

    TYPE replaceAll(CharSequence regex, CharSequence replacement);

    TYPE replace(CharSequence target, CharSequence replacement);

    TYPE[] split(CharSequence regex, int limit);

    TYPE[] split(CharSequence regex);

    TYPE toLowerCase(Locale locale);

    TYPE toLowerCase();

    TYPE toUpperCase(Locale locale);

    TYPE toUpperCase();

    TYPE intern();
}
