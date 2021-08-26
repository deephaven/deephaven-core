package io.deephaven.web.shared.ide;

import com.google.gwt.core.client.JavaScriptObject;
import elemental2.core.Global;
import elemental2.core.JsObject;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsArrayLike;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.web.shared.fu.JsArrays.setArray;

/**
 * A simple DTO for configuring a console session.
 */
@JsType(namespace = "dh")
public class ConsoleConfig implements Serializable {

    // Dispatcher addresses are optionally.
    private String dispatcherHost;
    private int dispatcherPort;

    private String jvmProfile;
    private String[] classpath;
    private int maxHeapMb;
    private String queryDescription;
    private boolean debug;
    private boolean detailedGCLogging;
    private boolean omitDefaultGcParameters;
    private String[] jvmArgs;
    // Using String[][] instead of a map to avoid polymorphism in RPC (it causes code bloat)
    private String[][] envVars;

    public ConsoleConfig() {
        maxHeapMb = 2048;
        queryDescription = "Web IDE console";
    }

    @JsProperty
    public String getDispatcherHost() {
        return dispatcherHost;
    }

    @JsProperty
    public void setDispatcherHost(String dispatcherHost) {
        this.dispatcherHost = dispatcherHost;
    }

    @JsProperty
    public int getDispatcherPort() {
        return dispatcherPort;
    }

    @JsProperty
    public void setDispatcherPort(int dispatcherPort) {
        this.dispatcherPort = dispatcherPort;
    }

    @JsProperty
    public String getJvmProfile() {
        return jvmProfile;
    }

    @JsProperty
    public void setJvmProfile(String jvmProfile) {
        this.jvmProfile = jvmProfile;
    }

    @JsProperty
    public Object getClasspath() {
        return classpath_();
    }

    @JsIgnore
    public String[] classpath_() {
        return classpath == null ? new String[0] : classpath;
    }

    @JsProperty
    public void setClasspath(Object classpath) {
        setArray(classpath, cp -> this.classpath = cp);
    }

    @JsIgnore
    public void classpath_(String[] classpath) {
        this.classpath = classpath;
    }

    @JsProperty
    public int getMaxHeapMb() {
        return maxHeapMb;
    }

    @JsProperty
    public void setMaxHeapMb(int maxHeapMb) {
        this.maxHeapMb = maxHeapMb;
    }

    @JsProperty
    public String getQueryDescription() {
        return queryDescription;
    }

    @JsProperty
    public void setQueryDescription(String queryDescription) {
        this.queryDescription = queryDescription;
    }

    @JsProperty
    public boolean isDebug() {
        return debug;
    }

    @JsProperty
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    @JsProperty
    public boolean isDetailedGCLogging() {
        return detailedGCLogging;
    }

    @JsProperty
    public void setDetailedGCLogging(boolean detailedGCLogging) {
        this.detailedGCLogging = detailedGCLogging;
    }

    @JsProperty
    public boolean isOmitDefaultGcParameters() {
        return omitDefaultGcParameters;
    }

    @JsProperty
    public void setOmitDefaultGcParameters(boolean omitDefaultGcParameters) {
        this.omitDefaultGcParameters = omitDefaultGcParameters;
    }

    @JsIgnore
    public String[] jvmArgs_() {
        return jvmArgs == null ? new String[0] : jvmArgs;
    }

    @JsProperty
    public Object getJvmArgs() {
        return jvmArgs_();
    }

    @JsProperty
    public void setJvmArgs(Object args) {
        setArray(args, a -> this.jvmArgs = a);
    }


    @JsIgnore
    public void jvmArgs_(String[] jvmArgs) {
        this.jvmArgs = jvmArgs;
    }

    @JsProperty
    public Object getEnvVars() {
        return envVars_();
    }

    @JsIgnore
    public String[][] envVars_() {
        return envVars == null ? new String[0][] : envVars;
    }

    @JsProperty
    public void setEnvVars(Object envVars) {
        if (envVars == null || envVars instanceof String[][]) {
            this.envVars = (String[][]) envVars; // defensive copy?
        } else if (envVars instanceof JavaScriptObject) {
            // this is actually javascript. We can do terrible things here and it's ok
            final int length = Array.getLength(envVars);
            this.envVars = new String[length][];
            for (int i = 0; i < length; i++) {
                JsArrayLike<Object> jsPair = Js.asArrayLike(envVars).getAnyAt(i).asArrayLike();
                if (jsPair.getLength() != 2) {
                    throw new IllegalArgumentException(
                        "Argument set doesn't contain two items: " + Global.JSON.stringify(jsPair));
                }
                String[] typed = new String[2];
                typed[0] = jsPair.getAnyAt(0).asString();
                typed[1] = jsPair.getAnyAt(1).asString();
                JsObject.freeze(typed);// make this immutable
                this.envVars[i] = typed;
            }
            JsObject.freeze(this.envVars);
        } else {
            throw new IllegalArgumentException("Not a String[][] or js [[]] " + envVars);

        }
    }

    @JsIgnore
    public void envVars_(String[][] envVars) {
        this.envVars = envVars;
    }

    @JsIgnore
    public Map<String, String> getEnvironmentVars() {
        final Map<String, String> map = new LinkedHashMap<>();
        if (envVars == null) {
            return map;
        }
        for (String[] envVar : envVars) {
            assert envVar.length == 2
                : "env vars must be arrays of arrays of length 2: [ [key, val], [k, v] ]";
            map.put(envVar[0], envVar[1]);
        }

        return map;
    }

    @Override
    public String toString() {
        return "ConsoleConfig{" +
            "dispatcherHost='" + dispatcherHost + '\'' +
            ", dispatcherPort=" + dispatcherPort +
            ", classpath=" + Arrays.toString(classpath) +
            ", maxHeapMb=" + maxHeapMb +
            ", queryDescription='" + queryDescription + '\'' +
            ", debug=" + debug +
            ", detailedGCLogging=" + detailedGCLogging +
            ", omitDefaultGcParameters=" + omitDefaultGcParameters +
            ", classpath=" + Arrays.toString(classpath_()) +
            ", jvmArgs=" + Arrays.toString(jvmArgs_()) +
            ", environmentVars=" + getEnvironmentVars() +
            '}';
    }
}
