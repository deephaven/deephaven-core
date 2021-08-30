package io.deephaven.appmode;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

@Immutable
@BuildableStyle
public abstract class ScriptApplication implements ApplicationConfig {

    public static final String TYPE = "script";

    public static Builder builder() {
        return ImmutableScriptApplication.builder();
    }

    public static ScriptApplication parse(Properties properties) {
        return builder()
                .isEnabled(ApplicationUtil.isEnabled(properties))
                .id(properties.getProperty("id"))
                .name(properties.getProperty("name"))
                .scriptType(properties.getProperty("scriptType").toLowerCase())
                .addFiles(ApplicationUtil.findFilesFrom(properties))
                .build();
    }

    public abstract String id();

    public abstract String name();

    public abstract List<Path> files();

    @Value.Default
    public boolean isEnabled() {
        return true;
    }

    public abstract String scriptType();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Value.Check
    final void checkNotEmpty() {
        if (files().isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one file to execute");
        }
    }

    public interface Builder {

        Builder isEnabled(boolean isEnabled);

        Builder id(String id);

        Builder name(String name);

        Builder addFiles(Path file);

        Builder addFiles(Path... files);

        Builder scriptType(String scriptType);

        ScriptApplication build();
    }
}
