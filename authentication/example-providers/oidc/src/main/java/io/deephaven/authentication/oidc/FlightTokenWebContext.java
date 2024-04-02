//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.authentication.oidc;

import org.pac4j.core.context.Cookie;
import org.pac4j.core.context.WebContext;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static io.deephaven.authentication.oidc.FlightTokenClient.FLIGHT_TOKEN_ATTRIBUTE_NAME;

/**
 * Simple WebContext that only exposes the provided string token as a request attribute. Flight's grpc/http contract
 * isn't flexible enough let most of this make sense, and Deephaven's specific authentication assumptions adds further
 * restrictions.
 */
class FlightTokenWebContext implements WebContext {
    private final String stringToken;

    public FlightTokenWebContext(String stringToken) {
        this.stringToken = stringToken;
    }

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
}
