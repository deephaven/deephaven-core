package io.deephaven.appmode;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

import java.util.Properties;

@Immutable
@SimpleStyle
public abstract class QSTApplication implements ApplicationConfig {

    public static final String TYPE = "qst";

    public static QSTApplication parse(Properties properties) {
        return of();
    }

    public static ImmutableQSTApplication of() {
        return ImmutableQSTApplication.of();
    }

    // TODO core#1080: QST structure undecided
    public boolean isEnabled() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
