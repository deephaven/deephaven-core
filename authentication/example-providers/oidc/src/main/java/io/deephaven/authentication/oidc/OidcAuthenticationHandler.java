package io.deephaven.authentication.oidc;

import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.AuthenticationRequestHandler;
import org.pac4j.core.client.Client;
import org.pac4j.core.config.Config;
import org.pac4j.core.context.Cookie;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.core.credentials.Credentials;
import org.pac4j.core.engine.DefaultSecurityLogic;
import org.pac4j.core.engine.SecurityGrantedAccessAdapter;
import org.pac4j.core.profile.ProfileManager;
import org.pac4j.core.profile.UserProfile;
import org.pac4j.http.client.direct.HeaderClient;
import org.pac4j.oidc.client.KeycloakOidcClient;
import org.pac4j.oidc.config.KeycloakOidcConfiguration;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static io.deephaven.authentication.oidc.FlightTokenClient.FLIGHT_TOKEN_ATTRIBUTE_NAME;

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
        WebContext context = new WebContext() {
            @Override
            public Optional<String> getRequestParameter(String name) {
                return Optional.empty();
            }

            @Override
            public Map<String, String[]> getRequestParameters() {
                return Collections.emptyMap();
            }

            @Override
            public Optional getRequestAttribute(String name) {
                if (name.equals(FLIGHT_TOKEN_ATTRIBUTE_NAME)) {
                    return Optional.of(stringToken);
                }
                return Optional.empty();
            }

            @Override
            public void setRequestAttribute(String name, Object value) {

            }

            @Override
            public Optional<String> getRequestHeader(String name) {
                return Optional.empty();
            }

            @Override
            public String getRequestMethod() {
                return null;
            }

            @Override
            public String getRemoteAddr() {
                return null;
            }

            @Override
            public void setResponseHeader(String name, String value) {

            }

            @Override
            public Optional<String> getResponseHeader(String name) {
                return Optional.empty();
            }

            @Override
            public void setResponseContentType(String content) {

            }

            @Override
            public String getServerName() {
                return null;
            }

            @Override
            public int getServerPort() {
                return 0;
            }

            @Override
            public String getScheme() {
                return null;
            }

            @Override
            public boolean isSecure() {
                return false;
            }

            @Override
            public String getFullRequestURL() {
                return null;
            }

            @Override
            public Collection<Cookie> getRequestCookies() {
                return Collections.emptySet();
            }

            @Override
            public void addResponseCookie(Cookie cookie) {

            }

            @Override
            public String getPath() {
                return null;
            }
        };
        SessionStore sessionStore = null;
//        logic.perform(context, sessionStore, cfg, new AccessGrantedOutcome(), (action, ctx) -> null, "", "", "", false);


        Client client = cfg.getClients().getClients().get(0);
        return client.getCredentials(context, sessionStore)
                .map(credentials -> client.getUserProfile(credentials, context, sessionStore))
                .map(profile -> new AuthContext.SuperUser());
    }

    public static class AccessGrantedOutcome implements SecurityGrantedAccessAdapter {

        @Override
        public Object adapt(WebContext context, SessionStore sessionStore, Collection<UserProfile> profiles, Object... parameters) throws Exception {
            return null;
        }
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
        config.setRealm("deephaven_core");
        config.setBaseUri("http://localhost:6060"); // TODO parameterize this
        config.setScope("openid email profile");

        KeycloakOidcClient client = new KeycloakOidcClient(config);
        client.setName("recipe-app-client");
        client.setConfiguration(config);
        client.setCallbackUrl("unused");
        client.init();

        FlightTokenClient flightTokenClient = new FlightTokenClient(client.getProfileCreator());

        cfg = new Config("/unused", flightTokenClient);
    }
}
