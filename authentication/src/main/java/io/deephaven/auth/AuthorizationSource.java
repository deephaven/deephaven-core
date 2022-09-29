/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.auth;

import com.google.rpc.Code;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.annotations.FinalDefault;

public interface AuthorizationSource {
    enum AuthorizationResult {
        ALLOW, BLOCK, UNDECIDED
    }

    AuthorizationResult hasPrivilege(Privilege privilege);

    @FinalDefault
    default void requirePrivilege(Privilege privilege) {
        if (hasPrivilege(privilege) != AuthorizationResult.ALLOW) {
            throw Exceptions.statusRuntimeException(
                    Code.PERMISSION_DENIED, "User does not have privilege " + privilege);
        }
    }
}
