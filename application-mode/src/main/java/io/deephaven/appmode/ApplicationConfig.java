/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.appmode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public interface ApplicationConfig {

    static boolean isCustomApplicationModeEnabled() {
        return ApplicationConfigImpl.APPLICATION_DIR != null;
    }

    /**
     * The custom application directory. Custom application mode must be enabled.
     *
     * @return the application dir
     * @see #isCustomApplicationModeEnabled()
     */
    static Path customApplicationDir() {
        if (!isCustomApplicationModeEnabled()) {
            throw new IllegalStateException(
                    String.format("Custom application mode is not enabled, please set system property '%s'",
                            ApplicationConfigImpl.APPLICATION_DIR_PROP));
        }
        final Path applicationDir;
        try {
            applicationDir = Paths.get(ApplicationConfigImpl.APPLICATION_DIR);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException(String.format("Invalid application directory '%s'",
                    ApplicationConfigImpl.APPLICATION_DIR));
        }
        return applicationDir;
    }

    /**
     * Parses the list of application configs found by searching the directory specified by the system property
     * {@code Application.dir}. A path is considered an application file when the name ends in {@code .app}, the file is
     * {@link Files#isReadable(Path) readable}, and the file is {@link Files#isRegularFile(Path, LinkOption...) regular}
     * and not a link. The resulting configs will be sorted lexicographically based on file name. Application mode must
     * be enabled.
     *
     * @return the list of application configs
     * @throws IOException on I/O exception
     * @throws ClassNotFoundException on class not found
     */
    static List<ApplicationConfig> find() throws IOException, ClassNotFoundException {
        return ApplicationConfigImpl.find(customApplicationDir());
    }

    /**
     * @return whether this particular application is enabled
     */
    boolean isEnabled();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(ScriptApplication script);

        void visit(StaticClassApplication<?> clazz);

        void visit(QSTApplication qst);

        void visit(DynamicApplication<?> advanced);
    }
}
