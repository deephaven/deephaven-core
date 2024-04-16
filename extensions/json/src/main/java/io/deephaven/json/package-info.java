/**
 * The deephaven JSON package presents a declarative and composable configuration layer for describing the structure of
 * a JSON value. It is meant to have sane defaults while also providing finer-grained configuration options for typical
 * scenarios. The primary purpose of this package is to provide a common layer that various consumers can use to parse
 * JSON values into appropriate Deephaven structures. As such (and by the very nature of JSON), these types represent a
 * superset of JSON. This package can also service other use cases where the JSON structuring is necessary (for example,
 * producing a JSON value from a Deephaven structure).
 *
 * <p>
 * Most of the configuration layers allow the user the choice of a "standard" option, a "strict" option, a "lenient"
 * option, and a "builder" option. The "standard" option is typically configured to allow missing values and to accept
 * the typically expected {@link io.deephaven.json.JsonValueTypes}, including
 * {@link io.deephaven.json.JsonValueTypes#NULL}. The "strict" option is typically configured to disallow missing values
 * and to accept the typically expected {@link io.deephaven.json.JsonValueTypes}, excluding
 * {@link io.deephaven.json.JsonValueTypes#NULL}. The "lenient" option is typically configured to allow missing values,
 * and to accept values a wide range of {@link io.deephaven.json.JsonValueTypes} by coercing atypical types into the
 * requested type (for example, parsing a {@link io.deephaven.json.JsonValueTypes#STRING} into an {@code int}). The
 * "builder" option allows the user fine-grained control over the behavior, and otherwise uses the "standard" options
 * when the user does not override.
 *
 * @see io.deephaven.json.Value
 */
package io.deephaven.json;
