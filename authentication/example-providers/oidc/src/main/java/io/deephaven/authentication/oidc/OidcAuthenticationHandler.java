//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.authentication.oidc;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.configuration.Configuration;
import org.pac4j.core.client.Client;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.WebContext;
import org.pac4j.oidc.client.KeycloakOidcClient;
import org.pac4j.oidc.config.KeycloakOidcConfiguration;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Functionally behaves as a pac4j DirectClient, but can lean on a profile creator as HeaderClient does.
 *
 * At this time, this is specific to Keycloak, as it is an easy IDP to quickly set up, and it can delegate to other
 * IDPs.
 */
public class OidcAuthenticationHandler implements AuthenticationRequestHandler {
    // Technically a general OIDC client only needs a discovery url, but this is helpful not only for
    private static final String KEYCLOAK_BASE_URL =
            Configuration.getInstance().getProperty("authentication.oidc.keycloak.url");
    private static final String KEYCLOAK_REALM =
            Configuration.getInstance().getProperty("authentication.oidc.keycloak.realm");
    private static final String KEYCLOAK_CLIENT_ID =
            Configuration.getInstance().getProperty("authentication.oidc.keycloak.clientId");

    private Config pac4jConfig;

    @Override
    public void initialize(String targetUrl) {
        // Configure Pac4j's keycloak client. Note that these actually just a plain openid configuration and client,
        // with some simplified setup
        KeycloakOidcConfiguration config = new KeycloakOidcConfiguration();
        config.setClientId(KEYCLOAK_CLIENT_ID);
        config.setRealm(KEYCLOAK_REALM);
        config.setBaseUri(KEYCLOAK_BASE_URL);
        config.setScope("openid email profile");

        KeycloakOidcClient client = new KeycloakOidcClient(config);
        client.setName("deephaven-app-client");
        client.setConfiguration(config);
        client.setCallbackUrl("unused");
        client.init();

        FlightTokenClient flightTokenClient = new FlightTokenClient(client.getProfileCreator());

        pac4jConfig = new Config("/unused", flightTokenClient);
    }

    @Override
    public String getAuthType() {
        return getClass().getName();
    }

    @Override
    public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload, HandshakeResponseListener listener)
            throws AuthenticationException {
        return validate(StandardCharsets.US_ASCII.decode(payload).toString());
    }

    @Override
    public Optional<AuthContext> login(String payload, MetadataResponseListener listener)
            throws AuthenticationException {
        return validate(payload);
    }

    private Optional<AuthContext> validate(String stringToken) {
        WebContext context = new FlightTokenWebContext(stringToken);

        // Inlined the contents of DefaultSecurityLogic that we care about, as we only have one direct client to try
        Client client = pac4jConfig.getClients().getClients().get(0);
        return client.getCredentials(context, null)
                .map(credentials -> client.getUserProfile(credentials, context, null))
                .map(profile -> new AuthContext.SuperUser());
    }
}
