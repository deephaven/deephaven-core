//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.proto.backplane.grpc.AuthenticationConstantsRequest;
import io.deephaven.server.grpc.UserAgentContext;
import io.deephaven.server.runner.DeephavenApiServerSingleUnauthenticatedBase;
import io.deephaven.server.runner.RpcServerStateInterceptor.RpcServerState;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class UserAgentTest extends DeephavenApiServerSingleUnauthenticatedBase {

    @Test
    public void userAgentContext() throws InterruptedException, TimeoutException {
        final RpcServerState state = serverStateInterceptor().newRpcServerState();
        // Any RPC will work
        // noinspection ResultOfMethodCallIgnored
        channel()
                .configBlocking()
                .withInterceptors(state.clientInterceptor())
                .getAuthenticationConstants(AuthenticationConstantsRequest.getDefaultInstance());
        state.awaitServerInvokeFinished(Duration.ofSeconds(3));
        assertThat(UserAgentContext.get(state.getCapturedContext()).orElse(null))
                .startsWith("ServerBuilderInProcessModule grpc-java-inprocess/");
    }
}
