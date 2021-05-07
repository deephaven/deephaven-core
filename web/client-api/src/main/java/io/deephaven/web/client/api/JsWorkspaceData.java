package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.JsString;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "Object", namespace = JsPackage.GLOBAL)
public class JsWorkspaceData {
    public String id;
    public Double version;
    public String name;
    public String owner;
    public String dataType;
    public String status;
    public JsArray<JsString> adminGroups;
    public JsArray<JsString> viewerGroups;

    public String data;

}
