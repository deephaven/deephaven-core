//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.proto;

import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceStub;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc.ConfigServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc.ConfigServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ConfigServiceGrpc.ConfigServiceStub;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceFutureStub;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceStub;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceFutureStub;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceFutureStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceFutureStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceBlockingStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceFutureStub;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;
import io.grpc.CallCredentials;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.MethodDescriptor;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.ClientCalls;

import java.util.Objects;
import java.util.function.Function;

/**
 * A Deephaven service helper for a {@link Channel channel}.
 */
public class DeephavenChannel {

    // Possibly useful for debugging, if we need to ensure that an RPC was created from a DeephavenChannel.
    private static final CallOptions.Key<DeephavenChannel> DEEPHAVEN_CHANNEL_KEY =
            CallOptions.Key.create("deephaven-channel");

    /**
     * @deprecated use {@link DeephavenChannel#withCallCredentials(CallCredentials)}
     */
    @Deprecated
    public static DeephavenChannel withCallCredentials(DeephavenChannel channel, CallCredentials callCredentials) {
        return channel.withCallCredentials(callCredentials);
    }

    /**
     * @deprecated use {@link DeephavenChannel#withInterceptors(ClientInterceptor...)}
     */
    @Deprecated
    public static DeephavenChannel withClientInterceptors(DeephavenChannel channel,
            ClientInterceptor... clientInterceptors) {
        return channel.withInterceptors(clientInterceptors);
    }

    private final Channel channel;
    private final CallCredentials credentials;

    public DeephavenChannel(Channel channel) {
        this(channel, null);
    }

    private DeephavenChannel(Channel channel, CallCredentials callCredentials) {
        this.channel = Objects.requireNonNull(channel);
        this.credentials = callCredentials;
    }

    /**
     * Returns a new channel that has the given interceptors attached to the underlying channel.
     *
     * @param clientInterceptors the interceptors
     * @return the Deephaven channel
     */
    public final DeephavenChannel withInterceptors(final ClientInterceptor... clientInterceptors) {
        return new DeephavenChannel(ClientInterceptors.intercept(channel, clientInterceptors), credentials);
    }

    /**
     * Returns a new channel that uses the given call credentials.
     *
     * @param callCredentials the call credentials
     * @return the Deephaven channel
     */
    public final DeephavenChannel withCallCredentials(final CallCredentials callCredentials) {
        return new DeephavenChannel(channel, callCredentials);
    }

    // Note: there is a fuzzy line between what sort of options are generally meant to be applied to all calls versus
    // single call sites. CallCredentials is the most obvious one that may be useful for all call sites. Executor,
    // authority, and compressor may be others. Deadline though, is clearly _not_ safe to use, as it is an absolute
    // point in time meant for a single, or single set of, call(s).
    // Here are the other options available in io.grpc.CallOptions... if desirable, we could extend DeephavenChannel to
    // support `withX` methods for these options:
    // Deadline deadline;
    // Executor executor;
    // String authority;
    // String compressorName;
    // Object[][] customOptions;
    // List<ClientStreamTracer.Factory> streamTracerFactories;
    // Boolean waitForReady;
    // Integer maxInboundMessageSize;
    // Integer maxOutboundMessageSize;
    // Integer onReadyThreshold;

    /**
     * The channel with any {@link #withInterceptors(ClientInterceptor...) interceptors} and {@link #callOptions() call
     * options} unconditionally applied.
     *
     * <p>
     * Note: the returned channel does not allow callers to easily override any {@link #callOptions() call options}. As
     * such, deriving a stub from the returned channel, or directly calling {@link ClientCalls} with it, can be
     * misleading. Stubs created via {@link #stub(Function)}, or the various pre-defined specific stubs, don't have this
     * issue.
     *
     * <p>
     * Callers of this are likely using this method in one of two patterns, and are encouraged to migrate as follows.
     *
     * <ol>
     * <li>Custom stub:
     * 
     * <pre>{@code
     * // old code, now deprecated
     * MyServiceStub myServiceStub = MyServiceGrpc.newStub(deephavenChannel.channel());
     *
     * // new code
     * MyServiceStub myServiceStub = deephavenChannel.stub(MyServiceGrpc::newStub);
     * }
     * </pre>
     * 
     * </li>
     * <li>Advanced client calls:
     * 
     * <pre>{@code
     * // old code, now deprecated
     * ClientCalls.blockingUnaryCall(
     *     deephavenChannel.channel(),
     *     methodDesc,
     *     CallOptions.DEFAULT.withX(...),
     *     myRequest);
     *
     * // new code
     * ClientCalls.blockingUnaryCall(
     *     deephavenChannel.channel2(),
     *     methodDesc,
     *     deephavenChannel.callOptions().withX(...),
     *     myRequest);
     * }
     * </pre>
     * 
     * </li>
     * </ol>
     *
     * @return the channel
     * @deprecated callers are encouraged to migrate to {@link #stub(Function)}, or {@link #channel2()} and
     *             {@link #callOptions()}
     */
    @Deprecated
    public final Channel channel() {
        return new AdaptedChannel();
    }

    /**
     * The channel with any {@link #withInterceptors(ClientInterceptor...) interceptors} applied.
     *
     * <p>
     * This is for more advanced use-cases where a pre-existing stub does not provide the utility necessary. Typically
     * used with {@link ClientCalls}.
     *
     * <pre>{@code
     * ClientCalls.blockingUnaryCall(
     *     deephavenChannel.channel2(),
     *     methodDesc,
     *     deephavenChannel.callOptions().withX(...),
     *     myRequest);
     * }
     * </pre>
     *
     * <p>
     * When using this, callers must use {@link #callOptions()} for correctness.
     *
     * @return the channel
     */
    public final Channel channel2() {
        return channel;
    }

    /**
     * The call options with any {@link #withCallCredentials(CallCredentials) CallCredentials} applied.
     *
     * <p>
     * This is for more advanced use-cases where a pre-existing stub does not provide the utility necessary. Typically
     * used with {@link ClientCalls}.
     *
     * <pre>{@code
     * ClientCalls.blockingUnaryCall(
     *     deephavenChannel.channel2(),
     *     methodDesc,
     *     deephavenChannel.callOptions().withX(...),
     *     myRequest);
     * }
     * </pre>
     *
     * <p>
     * When using this, callers must use {@link #channel2()} for correctness.
     *
     * @return the call options
     */
    public final CallOptions callOptions() {
        // See note in #stub() on why we don't pre-calculate this like we do for channel.
        return applyOptions(CallOptions.DEFAULT);
    }

    private CallOptions applyOptions(final CallOptions options) {
        return options
                .withOption(DEEPHAVEN_CHANNEL_KEY, this)
                .withCallCredentials(credentials);
    }

    /**
     * Creates a new stub, with the appropriate channel and call options applied.
     *
     * @param newStubFunction the new stub function
     * @return the stub, with appropriate channel and call options applied
     * @param <S> the concrete type of the stub.
     */
    public final <S extends AbstractStub<S>> S stub(final Function<Channel, S> newStubFunction) {
        // gRPC does not expose way to build a stub with an existing CallOptions - it needs to be built-up via fluent
        // API. (Otherwise, instead of storing each of the options, we would much prefer this class to just build up a
        // single CallOptions.)
        return newStubFunction.apply(channel)
                .withOption(DEEPHAVEN_CHANNEL_KEY, this)
                .withCallCredentials(credentials);
    }

    /**
     * Creates a new async session stub. Equivalent to {@code stub(SessionServiceGrpc::newStub)}.
     *
     * @return the async session stub
     * @see SessionServiceGrpc#newStub(Channel)
     */
    public final SessionServiceStub session() {
        return stub(SessionServiceGrpc::newStub);
    }

    /**
     * Creates a new async table stub. Equivalent to {@code stub(TableServiceGrpc::newStub)}.
     *
     * @return the async table stub
     * @see TableServiceGrpc#newStub(Channel)
     */
    public final TableServiceStub table() {
        return stub(TableServiceGrpc::newStub);
    }

    /**
     * Creates a new async console stub. Equivalent to {@code stub(ConsoleServiceGrpc::newStub)}.
     *
     * @return the async console stub
     * @see ConsoleServiceGrpc#newStub(Channel)
     */
    public final ConsoleServiceStub console() {
        return stub(ConsoleServiceGrpc::newStub);
    }

    /**
     * Creates a new async object stub. Equivalent to {@code stub(ObjectServiceGrpc::newStub)}.
     *
     * @return the async object stub
     * @see ObjectServiceGrpc#newStub(Channel)
     */
    public final ObjectServiceStub object() {
        return stub(ObjectServiceGrpc::newStub);
    }

    /**
     * Creates a new async application stub. Equivalent to {@code stub(ApplicationServiceGrpc::newStub)}.
     *
     * @return the async application stub
     * @see ApplicationServiceGrpc#newStub(Channel)
     */
    public final ApplicationServiceStub application() {
        return stub(ApplicationServiceGrpc::newStub);
    }

    /**
     * Creates a new async input table stub. Equivalent to {@code stub(InputTableServiceGrpc::newStub)}.
     *
     * @return the async input table stub
     * @see InputTableServiceGrpc#newStub(Channel)
     */
    public final InputTableServiceStub inputTable() {
        return stub(InputTableServiceGrpc::newStub);
    }

    /**
     * Creates a new async config stub. Equivalent to {@code stub(ConfigServiceGrpc::newStub)}.
     *
     * @return the async config stub
     * @see ConfigServiceGrpc#newStub(Channel)
     */
    public final ConfigServiceStub config() {
        return stub(ConfigServiceGrpc::newStub);
    }

    /**
     * Creates a new blocking session stub. Equivalent to {@code stub(SessionServiceGrpc::newBlockingStub)}.
     *
     * @return the blocking session stub
     * @see SessionServiceGrpc#newBlockingStub(Channel)
     */
    public final SessionServiceBlockingStub sessionBlocking() {
        return stub(SessionServiceGrpc::newBlockingStub);
    }

    /**
     * Creates a new blocking table stub. Equivalent to {@code stub(TableServiceGrpc::newBlockingStub)}.
     *
     * @return the blocking table stub
     * @see TableServiceGrpc#newBlockingStub(Channel)
     */
    public final TableServiceBlockingStub tableBlocking() {
        return stub(TableServiceGrpc::newBlockingStub);
    }

    /**
     * Creates a new blocking console stub. Equivalent to {@code stub(ConsoleServiceGrpc::newBlockingStub)}.
     *
     * @return the blocking console stub
     * @see ConsoleServiceGrpc#newBlockingStub(Channel)
     */
    public final ConsoleServiceBlockingStub consoleBlocking() {
        return stub(ConsoleServiceGrpc::newBlockingStub);
    }

    /**
     * Creates a new blocking object stub. Equivalent to {@code stub(ObjectServiceGrpc::newBlockingStub)}.
     *
     * @return the blocking object stub
     * @see ObjectServiceGrpc#newBlockingStub(Channel)
     */
    public final ObjectServiceBlockingStub objectBlocking() {
        return stub(ObjectServiceGrpc::newBlockingStub);
    }

    /**
     * Creates a new blocking application table stub. Equivalent to
     * {@code stub(ApplicationServiceGrpc::newBlockingStub)}.
     *
     * @return the blocking application table stub
     * @see ApplicationServiceGrpc#newBlockingStub(Channel)
     */
    public final ApplicationServiceBlockingStub applicationBlocking() {
        return stub(ApplicationServiceGrpc::newBlockingStub);
    }

    /**
     * Creates a new blocking input table stub. Equivalent to {@code stub(InputTableServiceGrpc::newBlockingStub)}.
     *
     * @return the blocking input table stub
     * @see InputTableServiceGrpc#newBlockingStub(Channel)
     */
    public final InputTableServiceBlockingStub inputTableBlocking() {
        return stub(InputTableServiceGrpc::newBlockingStub);
    }

    /**
     * Creates a new blocking config stub. Equivalent to {@code stub(ConfigServiceGrpc::newBlockingStub)}.
     *
     * @return the blocking config stub
     * @see ConfigServiceGrpc#newBlockingStub(Channel)
     */
    public final ConfigServiceBlockingStub configBlocking() {
        return stub(ConfigServiceGrpc::newBlockingStub);
    }

    /**
     * Creates a new future session stub. Equivalent to {@code stub(SessionServiceGrpc::newFutureStub)}.
     *
     * @return the future session stub
     * @see SessionServiceGrpc#newFutureStub(Channel)
     */
    public final SessionServiceFutureStub sessionFuture() {
        return stub(SessionServiceGrpc::newFutureStub);
    }

    /**
     * Creates a new future table stub. Equivalent to {@code stub(TableServiceGrpc::newFutureStub)}.
     *
     * @return the future table stub
     * @see TableServiceGrpc#newFutureStub(Channel)
     */
    public final TableServiceFutureStub tableFuture() {
        return stub(TableServiceGrpc::newFutureStub);
    }

    /**
     * Creates a new future console stub. Equivalent to {@code stub(ConsoleServiceGrpc::newFutureStub)}.
     *
     * @return the future console stub
     * @see ConsoleServiceGrpc#newFutureStub(Channel)
     */
    public final ConsoleServiceFutureStub consoleFuture() {
        return stub(ConsoleServiceGrpc::newFutureStub);
    }

    /**
     * Creates a new future object stub. Equivalent to {@code stub(ObjectServiceGrpc::newFutureStub)}.
     *
     * @return the future object stub
     * @see ObjectServiceGrpc#newFutureStub(Channel)
     */
    public final ObjectServiceFutureStub objectFuture() {
        return stub(ObjectServiceGrpc::newFutureStub);
    }

    /**
     * Creates a new future application stub. Equivalent to {@code stub(ApplicationServiceGrpc::newFutureStub)}.
     *
     * @return the future application stub
     * @see ApplicationServiceGrpc#newFutureStub(Channel)
     */
    public final ApplicationServiceFutureStub applicationFuture() {
        return stub(ApplicationServiceGrpc::newFutureStub);
    }

    /**
     * Creates a new future input table stub. Equivalent to {@code stub(InputTableServiceGrpc::newFutureStub)}.
     *
     * @return the future input table stub
     * @see InputTableServiceGrpc#newFutureStub(Channel)
     */
    public final InputTableServiceFutureStub inputTableFuture() {
        return stub(InputTableServiceGrpc::newFutureStub);
    }

    /**
     * Creates a new future config stub. Equivalent to {@code stub(ConfigServiceGrpc::newFutureStub)}.
     *
     * @return the future config stub
     * @see ConfigServiceGrpc#newFutureStub(Channel)
     */
    public final ConfigServiceFutureStub configFuture() {
        return stub(ConfigServiceGrpc::newFutureStub);
    }

    private final class AdaptedChannel extends Channel {

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
            return channel.newCall(methodDescriptor, applyOptions(callOptions));
        }

        @Override
        public String authority() {
            return channel.authority();
        }
    }
}
