package io.deephaven.util.auth;

import org.jetbrains.annotations.NotNull;

public class NormalUser implements AuthContext {

    @NotNull
    public final String getAuthRoleName() {
        return "NormalUser";
    }

    @NotNull
    public final String getAuthId() {
        return "normal";
    }
}
