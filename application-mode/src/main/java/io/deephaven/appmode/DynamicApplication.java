package io.deephaven.appmode;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Properties;

@Immutable
@SimpleStyle
public abstract class DynamicApplication<T extends ApplicationState.Factory> implements ApplicationConfig {

    public static final String TYPE = "dynamic";

    public static DynamicApplication<ApplicationState.Factory> parse(Properties properties)
            throws ClassNotFoundException {
        // noinspection unchecked
        Class<ApplicationState.Factory> clazz =
                (Class<ApplicationState.Factory>) Class.forName(properties.getProperty("class"));
        return ImmutableDynamicApplication.of(clazz, ApplicationUtil.isEnabled(properties));
    }

    public static <T extends ApplicationState.Factory> DynamicApplication<T> of(Class<T> clazz, boolean isEnabled) {
        return ImmutableDynamicApplication.of(clazz, isEnabled);
    }

    @Parameter
    public abstract Class<T> clazz();

    @Parameter
    public abstract boolean isEnabled();

    public final ApplicationState create(ApplicationState.Listener appStateListener)
            throws InstantiationException, IllegalAccessException {
        return clazz().newInstance().create(appStateListener);
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkClazz() {
        if (!ApplicationState.Factory.class.isAssignableFrom(clazz())) {
            throw new IllegalArgumentException(
                    String.format("clazz should extend '%s'", ApplicationState.Factory.class));
        }
    }
}
