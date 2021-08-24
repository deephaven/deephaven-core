package io.deephaven.process;

import io.deephaven.properties.SplayedPath;
import java.io.IOException;
import java.util.Collections;
import org.immutables.value.Value;

/**
 * Represents a free-form {@link io.deephaven.properties.PropertySet} that is parsed via
 * {@link SplayedPath#toStringMap()} for inclusion at {@link ProcessInfo#getHostPathInfo()}. This
 * allows for a variety of use-cases where information can be attached to a host at install,
 * upgrade, testing, or other time.
 */
@Value.Immutable
@Wrapped
abstract class _HostPathInfo extends StringMapWrapper {

    static HostPathInfo of(SplayedPath splayedPath) throws IOException {
        return splayedPath.exists() ? HostPathInfo.of(splayedPath.toStringMap())
            : HostPathInfo.of(Collections.emptyMap());
    }
}
