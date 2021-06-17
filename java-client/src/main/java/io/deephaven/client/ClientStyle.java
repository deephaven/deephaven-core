package io.deephaven.client;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Target({ElementType.PACKAGE, ElementType.TYPE})
@Retention(RetentionPolicy.CLASS)
@Value.Style(visibility = ImplementationVisibility.PACKAGE,
    defaults = @Value.Immutable(copy = false), strictBuilder = true)
public @interface ClientStyle {
}
