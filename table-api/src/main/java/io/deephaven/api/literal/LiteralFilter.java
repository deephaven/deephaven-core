//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.literal;

import io.deephaven.api.filter.Filter;

public interface LiteralFilter extends Literal, Filter {

    LiteralFilter invert();
}
