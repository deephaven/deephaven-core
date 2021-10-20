package io.deephaven.uri;

import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteUriTest {

    private static final DeephavenTarget TARGET = DeephavenTarget.builder()
            .isTLS(true)
            .host("host")
            .build();

    private static final DeephavenTarget TARGET_PORT = DeephavenTarget.builder()
            .isTLS(true)
            .host("host")
            .port(31337)
            .build();

    private static final DeephavenTarget GATEWAY = DeephavenTarget.builder()
            .isTLS(true)
            .host("gateway")
            .build();

    private static final DeephavenTarget GATEWAY_PORT = DeephavenTarget.builder()
            .isTLS(true)
            .host("gateway")
            .port(42)
            .build();

    private static final DeephavenTarget GATEWAY_1 = DeephavenTarget.builder()
            .isTLS(true)
            .host("gateway-1")
            .build();

    private static final DeephavenTarget GATEWAY_2 = DeephavenTarget.builder()
            .isTLS(false)
            .host("gateway-2")
            .build();

    private static final ApplicationUri LOCAL_APP = ApplicationUri.of("appId", "fieldName");

    private static final QueryScopeUri QUERY = QueryScopeUri.of("variable");

    private static final FieldUri FIELD = FieldUri.of("fieldName");

    @Test
    void applicationField() {
        check("dh://host/app/appId/field/fieldName", LOCAL_APP.target(TARGET));
    }

    @Test
    void field() {
        check("dh://host/field/fieldName", FIELD.target(TARGET));
    }

    @Test
    void queryScope() {
        check("dh://host/scope/variable", QUERY.target(TARGET));
    }

    @Test
    void applicationFieldPort() {
        check("dh://host:31337/app/appId/field/fieldName", LOCAL_APP.target(TARGET_PORT));
    }

    @Test
    void fieldPort() {
        check("dh://host:31337/field/fieldName", FIELD.target(TARGET_PORT));
    }

    @Test
    void queryScopePort() {
        check("dh://host:31337/scope/variable", QUERY.target(TARGET_PORT));
    }

    @Test
    void proxyApplicationField() {
        check("dh://gateway/dh/host/app/appId/field/fieldName", LOCAL_APP.target(TARGET).target(GATEWAY));
    }

    @Test
    void proxyField() {
        check("dh://gateway/dh/host/field/fieldName", FIELD.target(TARGET).target(GATEWAY));
    }

    @Test
    void proxyQueryScope() {
        check("dh://gateway/dh/host/scope/variable", QUERY.target(TARGET).target(GATEWAY));
    }

    @Test
    void proxyApplicationFieldPort() {
        check("dh://gateway/dh/host:31337/app/appId/field/fieldName", LOCAL_APP.target(TARGET_PORT).target(GATEWAY));
    }

    @Test
    void proxyFieldPort() {
        check("dh://gateway/dh/host:31337/field/fieldName", FIELD.target(TARGET_PORT).target(GATEWAY));
    }

    @Test
    void proxyQueryScopePort() {
        check("dh://gateway/dh/host:31337/scope/variable", QUERY.target(TARGET_PORT).target(GATEWAY));
    }

    @Test
    void proxyPortApplicationField() {
        check("dh://gateway:42/dh/host/app/appId/field/fieldName", LOCAL_APP.target(TARGET).target(GATEWAY_PORT));
    }

    @Test
    void proxyPortField() {
        check("dh://gateway:42/dh/host/field/fieldName", FIELD.target(TARGET).target(GATEWAY_PORT));
    }

    @Test
    void proxyPortQueryScope() {
        check("dh://gateway:42/dh/host/scope/variable", QUERY.target(TARGET).target(GATEWAY_PORT));
    }

    @Test
    void proxyPortApplicationFieldPort() {
        check("dh://gateway:42/dh/host:31337/app/appId/field/fieldName",
                LOCAL_APP.target(TARGET_PORT).target(GATEWAY_PORT));
    }

    @Test
    void proxyPortFieldPort() {
        check("dh://gateway:42/dh/host:31337/field/fieldName", FIELD.target(TARGET_PORT).target(GATEWAY_PORT));
    }

    @Test
    void proxyPortQueryScopePort() {
        check("dh://gateway:42/dh/host:31337/scope/variable",
                QUERY.target(TARGET_PORT).target(GATEWAY_PORT));
    }

    @Test
    void doubleProxy() {
        check("dh://gateway-1/dh+plain/gateway-2/dh/host/field/fieldName",
                FIELD.target(TARGET).target(GATEWAY_2).target(GATEWAY_1));
    }

    @Test
    void remoteRaw() {
        final RawUri uri = RawUri.of(URI.create("some-protocol://user@some-host:15/some-path?someQuery=ok#myfragment"));
        final RemoteUri remoteUri = uri.target(TARGET);
        check("dh://host/some-protocol/%2F%2Fuser%40some-host%3A15%2Fsome-path%3FsomeQuery%3Dok%23myfragment",
                remoteUri);
    }

    static void check(String uriString, RemoteUri uri) {
        assertThat(uri.toString()).isEqualTo(uriString);
        final RemoteUri remoteUri = RemoteUri.of(StandardCreator.INSTANCE, URI.create(uriString));
        assertThat(remoteUri).isEqualTo(uri);
    }
}
