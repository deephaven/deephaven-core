package io.deephaven.authentication.oidc;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.AuthenticationRequestHandler;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.engine.DefaultSecurityLogic;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.http.client.direct.HeaderClient;
import org.pac4j.oidc.client.KeycloakOidcClient;
import org.pac4j.oidc.config.KeycloakOidcConfiguration;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Functionally behaves as a pac4j DirectClient, but can lean on a profile creator as BearerClient does
 */
public class OidcAuthenticationHandler implements AuthenticationRequestHandler {
    private Config cfg;

    @Override
    public String getAuthType() {
        return OidcAuthenticationHandler.class.getName();
    }

    @Override
    public Optional<AuthContext> login(long protocolVersion, ByteBuffer payload, HandshakeResponseListener listener) throws AuthenticationException {
        return validate(StandardCharsets.US_ASCII.decode(payload).toString());
    }

    @Override
    public Optional<AuthContext> login(String payload, MetadataResponseListener listener) throws AuthenticationException {
        return validate(payload);
    }

    private Optional<AuthContext> validate(String stringToken) {
        DefaultSecurityLogic logic = new DefaultSecurityLogic();

        logic.setProfileManagerFactory(DeephavenProfileManager::new);
        WebContext context = null;
        SessionStore sessionStore = null;
        logic.perform(context, sessionStore, cfg, new AccessGrantedOutcome(), (action, context) -> null, "", "", "", false);


        cfg.getClients().getClients().get(0).getCredentials()

        return Optional.empty();
    }

    public static class DeephavenProfileManager extends ProfileManager {

        public DeephavenProfileManager(WebContext context, SessionStore sessionStore) {
            super(context, sessionStore);
        }

    }

    @Override
    public void initialize(String targetUrl) {
        KeycloakOidcConfiguration config = new KeycloakOidcConfiguration();
        config.setClientId("app");
        config.setRealm("deephaven_realm");
        config.setBaseUri("http://localhost:6060"); // TODO parameterize this
        config.setScope("openid email profile");

        KeycloakOidcClient client = new KeycloakOidcClient(config);
        client.setName("recipe-app-client");
        client.setConfiguration(config);
        client.setCallbackUrl("unused");
        client.init();

        //TODO probably will use a custom client here
        HeaderClient bearerClient =
                new HeaderClient("Authorization", getAuthType() + " ", client.getProfileCreator());

        cfg = new Config("/unused", bearerClient);
    }
}
