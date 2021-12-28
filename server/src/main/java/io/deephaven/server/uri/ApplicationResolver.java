package io.deephaven.server.uri;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.server.appmode.ApplicationStates;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.uri.DeephavenUri;
import io.deephaven.uri.resolver.UriResolver;
import io.deephaven.uri.resolver.UriResolversInstance;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * The application table resolver is able to resolve {@link ApplicationUri application URIs}.
 *
 * <p>
 * For example, {@code dh:///app/my_app/field/my_field}.
 *
 * @see ApplicationUri application URI format
 */
public final class ApplicationResolver implements UriResolver {

    public static ApplicationResolver get() {
        return UriResolversInstance.get().find(ApplicationResolver.class).get();
    }

    private final ApplicationStates states;

    @Inject
    public ApplicationResolver(ApplicationStates states) {
        this.states = Objects.requireNonNull(states);
    }

    @Override
    public Set<String> schemes() {
        return Collections.singleton(DeephavenUri.LOCAL_SCHEME);
    }

    @Override
    public boolean isResolvable(URI uri) {
        return ApplicationUri.isWellFormed(uri);
    }

    @Override
    public Object resolve(URI uri) {
        final Field<Object> field = resolve(ApplicationUri.of(uri));
        return field == null ? null : field.value();
    }

    public Field<Object> resolve(ApplicationUri uri) {
        return resolve(uri.applicationId(), uri.fieldName());
    }

    public Field<Object> resolve(String applicationId, String fieldName) {
        final ApplicationState app = states.getApplicationState(applicationId).orElse(null);
        return app == null ? null : app.getField(fieldName);
    }
}
