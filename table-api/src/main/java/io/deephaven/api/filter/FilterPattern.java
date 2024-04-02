//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A filter based on a regular-expression {@link Pattern}, compatible with any column types that are compatible with
 * {@link CharSequence}.
 *
 * <p>
 * In the {@link Mode#MATCHES MATCHES} case, the logic is equivalent to
 * {@code value != null && (invertPattern() ^ pattern().matcher(value).matches())}.
 *
 * <p>
 * In the {@link Mode#FIND FIND} case, the logic is equivalent to
 * {@code value != null && (invertPattern() ^ pattern().matcher(value).find())}.
 *
 * <p>
 * This filter will never match {@code null} values.
 */
@Immutable
@BuildableStyle
public abstract class FilterPattern extends FilterBase {

    public static Builder builder() {
        return ImmutableFilterPattern.builder();
    }

    public static FilterPattern of(Expression expression, Pattern pattern, Mode mode, boolean invertPattern) {
        return builder()
                .expression(expression)
                .pattern(pattern)
                .mode(mode)
                .invertPattern(invertPattern)
                .build();
    }

    public abstract Expression expression();

    public abstract Pattern pattern();

    public abstract Mode mode();

    @Default
    public boolean invertPattern() {
        return false;
    }

    @Override
    public final FilterNot<FilterPattern> invert() {
        return Filter.not(this);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    // Implementation note: toString is needed because Pattern#toString does not output flags

    @Override
    public final String toString() {
        return "FilterPattern("
                + expression()
                + ", " + pattern().pattern()
                + ", " + pattern().flags()
                + ", " + mode()
                + ", " + invertPattern()
                + ")";
    }

    // Implementation note: equals / hashCode are needed because Pattern does not override equals / hashCode

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof FilterPattern)) {
            return false;
        }
        final FilterPattern other = (FilterPattern) obj;
        return mode() == other.mode()
                && invertPattern() == other.invertPattern()
                && pattern().flags() == other.pattern().flags()
                && pattern().pattern().equals(other.pattern().pattern())
                && expression().equals(other.expression());
    }

    @Override
    public final int hashCode() {
        int h = 5381;
        h += (h << 5) + expression().hashCode();
        h += (h << 5) + pattern().pattern().hashCode();
        h += (h << 5) + Integer.hashCode(pattern().flags());
        h += (h << 5) + mode().hashCode();
        h += (h << 5) + Boolean.hashCode(invertPattern());
        return h;
    }

    /**
     * The pattern mode
     */
    public enum Mode {
        /**
         * Matches any subsequence of the {@code input} against the {@code pattern}, uses {@link Matcher#find()}.
         */
        FIND,

        /**
         * Matches the entire {@code input} against the {@code pattern}, uses {@link Matcher#matches()}.
         */
        MATCHES
    }

    public interface Builder {
        Builder expression(Expression expression);

        Builder pattern(Pattern pattern);

        Builder mode(Mode mode);

        Builder invertPattern(boolean invertPattern);

        FilterPattern build();
    }
}
