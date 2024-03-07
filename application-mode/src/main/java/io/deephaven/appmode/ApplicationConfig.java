//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.appmode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public interface ApplicationConfig {

    /**
     * Returns true if the configuration property {@value ApplicationConfigImpl#APPLICATION_DIR_PROP} is set, or if the
     * {@value ApplicationConfigImpl#DEFAULT_APP_DIRNAME} exists in the configuration directory.
     *
     * @return true if custom application mode is enabled
     */
    static boolean isCustomApplicationModeEnabled() {
        return ApplicationConfigImpl.applicationDir().isPresent();
    }

    /**
     * The custom application directory. Returns the configuration property
     * {@value ApplicationConfigImpl#APPLICATION_DIR_PROP} if it set, otherwise it returns the
     * {@value ApplicationConfigImpl#DEFAULT_APP_DIRNAME} from the configuration directory if it exists, otherwise it
     * throws an {@link IllegalStateException}.
     *
     * @return the application dir
     * @see #isCustomApplicationModeEnabled()
     */
    static Path customApplicationDir() {
        final Optional<Path> applicationDir = ApplicationConfigImpl.applicationDir();
        if (applicationDir.isEmpty()) {
            throw new IllegalStateException(
                    String.format("Custom application mode is not enabled, please set the configuration property '%s'",
                            ApplicationConfigImpl.APPLICATION_DIR_PROP));
        }
        return applicationDir.get();
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
