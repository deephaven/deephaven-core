package io.deephaven.python;

import java.lang.invoke.MethodHandles;
import java.util.Objects;
import org.jpy.PyLibInitializer;

public class ASTHelperSetup {

    private static final String NAME = "ast_helper";
    private static final String RESOURCE = NAME + ".py";

    private static volatile ASTHelper instance;

    public static ASTHelper create() {
        if (!PyLibInitializer.isPyLibInitialized()) {
            throw new IllegalStateException("PyLib not initialized");
        }
        return PyModuleFromResource
            .load(MethodHandles.lookup().lookupClass(), NAME, RESOURCE)
            .createProxy(ASTHelper.class);
    }

    public static ASTHelper getInstance() {
        ASTHelper local;
        if ((local = instance) == null) {
            synchronized (ASTHelperSetup.class) {
                if ((local = instance) == null) {
                    local = (instance = Objects.requireNonNull(create()));
                }
            }
        }
        return local;
    }
}
