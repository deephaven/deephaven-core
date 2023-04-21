package io.deephaven.api.filter;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.expression.Expression;
import org.immutables.value.Value.Immutable;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Immutable
@BuildableStyle
public abstract class FilterPattern extends FilterBase {

    public static Builder builder() {
        return ImmutableFilterPattern.builder();
    }

    public static FilterPattern of(Expression expression, Pattern pattern, Mode mode) {
        return builder()
                .expression(expression)
                .pattern(pattern)
                .mode(mode)
                .build();
    }

    public abstract Expression expression();

    public abstract Pattern pattern();

    public abstract Mode mode();

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
        return "FilterPattern{"
                + "expression=" + expression()
                + ", pattern=" + pattern().pattern()
                + ", patternFlags=" + pattern().flags()
                + ", mode=" + mode()
                + "}";
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
        return expression().equals(other.expression())
                && pattern().pattern().equals(other.pattern().pattern())
                && pattern().flags() == other.pattern().flags()
                && mode() == other.mode();
    }

    @Override
    public final int hashCode() {
        int h = 5381;
        h += (h << 5) + expression().hashCode();
        h += (h << 5) + pattern().pattern().hashCode();
        h += (h << 5) + Integer.hashCode(pattern().flags());
        h += (h << 5) + mode().hashCode();
        return h;
    }

    public enum Mode {
        /**
         * Matches the entire {@code input} against the {@code pattern}, uses {@link Matcher#matches()}.
         */
        FIND,

        /**
         * Matches any subsequence of the {@code input} against the {@code pattern}, uses {@link Matcher#find()}.
         */
        MATCHES
    }

    public interface Builder {
        Builder expression(Expression expression);

        Builder pattern(Pattern pattern);

        Builder mode(Mode mode);

        FilterPattern build();
    }
}
