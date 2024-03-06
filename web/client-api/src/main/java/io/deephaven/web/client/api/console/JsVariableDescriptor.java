//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
    public String type;
    @JsNullable
    public String id;
    @JsNullable
    public String name;
}
