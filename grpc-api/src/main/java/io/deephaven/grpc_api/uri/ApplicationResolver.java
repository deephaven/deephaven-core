package io.deephaven.grpc_api.uri;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.appmode.ApplicationStates;
import io.deephaven.uri.ApplicationUri;
import io.deephaven.uri.DeephavenUri;

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
public final class ApplicationResolver implements TableResolver {

    public static ApplicationResolver get() {
        return TableResolversInstance.get().find(ApplicationResolver.class).get();
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
    public Table resolve(URI uri) {
        return resolve(ApplicationUri.of(uri));
    }

    public Table resolve(ApplicationUri uri) {
        return resolve(uri.applicationId(), uri.fieldName());
    }

    public Table resolve(String applicationId, String fieldName) {
        final ApplicationState app = states.getApplicationState(applicationId).orElse(null);
        if (app == null) {
            return null;
        }
        final Field<Object> field = app.getField(fieldName);
        if (field == null) {
            return null;
        }
        return asTable(field.value(), applicationId, fieldName);
    }

    private Table asTable(Object value, String context, String fieldName) {
        if (value == null || value instanceof Table) {
            return (Table) value;
        }
        throw new IllegalArgumentException(
                String.format("Field '%s' in '%s' is not a Table, is %s", fieldName, context, value.getClass()));
    }
}
