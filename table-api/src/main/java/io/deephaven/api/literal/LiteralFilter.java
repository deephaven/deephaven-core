package io.deephaven.api.literal;

import io.deephaven.api.filter.Filter;

public interface LiteralFilter extends Literal, Filter {

    LiteralFilter invert();
}
