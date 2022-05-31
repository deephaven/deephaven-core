package io.deephaven.server.appmode;

import com.google.common.base.Objects;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.server.console.ScopeTicketResolver;
import io.deephaven.proto.backplane.grpc.Ticket;
import org.jetbrains.annotations.Nullable;

/**
 * Our unique identifier for a field. Used to debounce frequent updates and to identify when a field is replaced.
 */
public class AppFieldId {
    private static final String SCOPE_ID = "scope";

    /**
     * The application that this field is defined under, or null if this is a fabricated query scope field.
     */
    final ApplicationState app;
    final String fieldName;

    static AppFieldId from(final ApplicationState app, final String fieldName) {
        return new AppFieldId(app, fieldName);
    }

    public static AppFieldId fromScopeName(final String fieldName) {
        return new AppFieldId(null, fieldName);
    }

    private AppFieldId(@Nullable final ApplicationState app, final String fieldName) {
        this.app = app;
        if (this.app != null && this.app.id().equals(SCOPE_ID)) {
            throw new IllegalArgumentException(String.format(
                    "Applications cannot re-use the id '%s'; it is reserved for QueryScope use only", SCOPE_ID));
        }
        this.fieldName = fieldName;
    }

    public Ticket getTicket() {
        if (app == null) {
            return ScopeTicketResolver.ticketForName(fieldName);
        }
        return ApplicationTicketResolver.ticketForName(app, fieldName);
    }

    public String applicationId() {
        return (app == null) ? SCOPE_ID : app.id();
    }

    public String applicationName() {
        return (app == null) ? "" : app.name();
    }

    @Override
    public String toString() {
        return "AppFieldId{appId=" + (app == null ? "" : app.id()) + ", fieldName=" + fieldName + "}";
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(app == null ? null : app.id(), fieldName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final AppFieldId fid = (AppFieldId) o;
        // note instance equality on the app; either both need to be null, or must point to the same instance
        return app == fid.app && fieldName.equals(fid.fieldName);
    }
}
