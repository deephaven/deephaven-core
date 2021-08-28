package io.deephaven.appmode;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Properties;

@Immutable
@SimpleStyle
public abstract class StaticClassApplication<T extends Application.Factory> implements ApplicationConfig {

    public static final String TYPE = "static";

    public static StaticClassApplication<Application.Factory> parse(Properties properties)
            throws ClassNotFoundException {
        // noinspection unchecked
        Class<Application.Factory> clazz = (Class<Application.Factory>) Class.forName(properties.getProperty("class"));
        return ImmutableStaticClassApplication.of(clazz, ApplicationUtil.isEnabled(properties));
    }

    public static <T extends Application.Factory> StaticClassApplication<T> of(Class<T> clazz, boolean isEnabled) {
        return ImmutableStaticClassApplication.of(clazz, isEnabled);
    }

    @Parameter
    public abstract Class<T> clazz();

    @Parameter
    public abstract boolean isEnabled();

    public final Application create() throws InstantiationException, IllegalAccessException {
        return clazz().newInstance().create();
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkClazz() {
        if (!Application.Factory.class.isAssignableFrom(clazz())) {
            throw new IllegalArgumentException(
                    String.format("clazz should extend '%s'", Application.Factory.class));
        }
    }
}
