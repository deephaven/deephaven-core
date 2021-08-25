package io.deephaven.db.v2.utils.rsp.container;

/**
 * A ShortConsumer receives the short values contained in a data structure. Each value is visited once.
 * <p>
 * Usage:
 *
 * <pre>
 * {@code
 * bitmap.forEach(new ShortConsumer() {
 *   public boolean accept(short value) {
 *     // do something here
 *   }
 * });
 * }
 * </pre>
 */

public interface ShortConsumer {
    /**
     * Provides a value to this consumer. A false return value indicates that the application providing values to this
     * consumer should not invoke it again.
     *
     * @param value the short value
     * @return false if don't want any more values after this one, true otherwise.
     */
    boolean accept(short value);
}
