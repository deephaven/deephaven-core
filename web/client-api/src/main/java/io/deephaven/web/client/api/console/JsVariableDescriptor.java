//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.console;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Specifies a type and either id or name (but not both).
 */
@TsInterface
@JsType(namespace = "dh.ide", name = "VariableDescriptor")
public class JsVariableDescriptor {
    /**
     * The variable type.
     */
    public String type;

    /**
     * The variable id.
     *
     * Mutually exclusive with {@link #name}.
     */
    @JsNullable
    public String id;

    /**
     * The variable name.
     *
     * Mutually exclusive with {@link #id}.
     */
    @JsNullable
    public String name;
}
