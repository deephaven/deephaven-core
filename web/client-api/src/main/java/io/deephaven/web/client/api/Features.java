//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import io.deephaven.web.client.api.tree.JsTreeTable;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Collection of feature flags that the JS API advertises. All must be nullable booleans, and if listed, the value is
 * true.
 * <p>
 * Preferred format is to first list the type or category of feature, then the feature name. Docs should link
 * bidirectionally with the feature in question.
 *
 * Exposed as {@link CoreClient#FEATURES}, which may be null in older releases.
 */
@JsType(namespace = "dh")
public class Features {
    /**
     * {@link io.deephaven.web.client.api.tree.JsTreeTable} support for providing a number rather than a boolean when
     * {@link io.deephaven.web.client.api.tree.JsTreeTable#expand(JsTreeTable.RowReferenceUnion, JsTreeTable.ExpandDescendantsUnion)
     * expanding} to signify expanding to a depth relative to the given element.
     */
    @JsNullable
    public final Boolean treeTableExpandToDepth = true;
}
