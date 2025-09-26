//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

/**
 * A Selectable that wraps another selectable, but reports a serial result.
 */
class SelectableWithSerial extends WrappedSelectable {
    SelectableWithSerial(Selectable wrapped) {
        super(wrapped);
    }

    @Override
    public Boolean isSerial() {
        return true;
    }
}
