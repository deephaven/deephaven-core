//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.authentication.oidc;

import org.pac4j.core.client.DirectClient;
import org.pac4j.core.credentials.TokenCredentials;
import org.pac4j.core.credentials.authenticator.Authenticator;
import org.pac4j.core.profile.creator.ProfileCreator;

/**
 * Reads the provided flight token and uses it to build a profile.
 */
public class FlightTokenClient extends DirectClient {

    public static final String FLIGHT_TOKEN_ATTRIBUTE_NAME = "flight-token";

    public FlightTokenClient(ProfileCreator profileCreator) {
        defaultProfileCreator(profileCreator);
        defaultAuthenticator(Authenticator.ALWAYS_VALIDATE);
        defaultCredentialsExtractor((context, sessionStore) -> {
            return context.getRequestAttribute(FLIGHT_TOKEN_ATTRIBUTE_NAME)
                    .map(token -> new TokenCredentials((String) token));
        });
    }

    @Override
    protected void internalInit(boolean forceReinit) {}
}
