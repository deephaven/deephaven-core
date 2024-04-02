//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import com.google.auto.service.AutoService;
import io.deephaven.uri.DeephavenTarget;
import io.deephaven.uri.DeephavenUri;
import io.grpc.NameResolver;
import io.grpc.NameResolver.Args;
import io.grpc.NameResolverProvider;
import io.grpc.internal.DnsNameResolverProvider;

import java.net.URI;

public final class DeephavenTargetNameResolverProviders {

    // TODO(deephaven-core#1489): Support service discovery for DeephavenTarget

    private static final DnsNameResolverProvider DNS_NAME_RESOLVER_PROVIDER = new DnsNameResolverProvider();

    private static URI deephavenToDnsTarget(URI targetUri) {
        final DeephavenTarget deephavenTarget = DeephavenTarget.of(targetUri);
        // https://grpc.github.io/grpc/core/md_doc_naming.html
        return deephavenTarget.port().isPresent()
                ? URI.create(String.format("dns:///%s:%d", deephavenTarget.host(), deephavenTarget.port().getAsInt()))
                : URI.create(String.format("dns:///%s", deephavenTarget.host()));
    }

    /**
     * Provides {@link NameResolver} support for plaintext {@link DeephavenTarget}.
     */
    @AutoService(NameResolverProvider.class)
    public static final class Plaintext extends NameResolverProvider {

        private static Args setDefaultPort(Args args) {
            return args.toBuilder().setDefaultPort(ChannelHelper.DEFAULT_PLAINTEXT_PORT).build();
        }

        @Override
        protected boolean isAvailable() {
            return true;
        }

        @Override
        protected int priority() {
            return 5;
        }

        @Override
        public NameResolver newNameResolver(URI targetUri, Args args) {
            if (!DeephavenUri.PLAINTEXT_SCHEME.equals(targetUri.getScheme())) {
                return null;
            }
            return DNS_NAME_RESOLVER_PROVIDER.newNameResolver(deephavenToDnsTarget(targetUri), setDefaultPort(args));
        }

        @Override
        public String getDefaultScheme() {
            return DeephavenUri.PLAINTEXT_SCHEME;
        }
    }


    /**
     * Provides {@link NameResolver} support for secure {@link DeephavenTarget}.
     */
    @AutoService(NameResolverProvider.class)
    public static final class Secure extends NameResolverProvider {

        private static Args setDefaultPort(Args args) {
            return args.toBuilder().setDefaultPort(ChannelHelper.DEFAULT_TLS_PORT).build();
        }

        @Override
        protected boolean isAvailable() {
            return true;
        }

        @Override
        protected int priority() {
            return 5;
        }

        @Override
        public NameResolver newNameResolver(URI targetUri, Args args) {
            if (!DeephavenUri.SECURE_SCHEME.equals(targetUri.getScheme())) {
                return null;
            }
            return DNS_NAME_RESOLVER_PROVIDER.newNameResolver(deephavenToDnsTarget(targetUri), setDefaultPort(args));
        }

        @Override
        public String getDefaultScheme() {
            return DeephavenUri.SECURE_SCHEME;
        }
    }
}
