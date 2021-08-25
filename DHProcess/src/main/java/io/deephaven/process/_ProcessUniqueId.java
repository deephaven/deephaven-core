package io.deephaven.process;

import org.immutables.value.Value;

/**
 * The globally unique ID for a process. This is <b>not</b> the same as a "pid" / process-id.
 */
@Value.Immutable
@Wrapped
abstract class _ProcessUniqueId extends Wrapper<String> {

}
