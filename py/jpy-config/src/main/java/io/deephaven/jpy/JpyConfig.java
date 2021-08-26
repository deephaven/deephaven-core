package io.deephaven.jpy;

import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * This class encapsulates the configuration data and invocation of {/@link PyLibInitializer#initPyLib(String, String,
 * String)}, {/@link PyLib#setProgramName(String)}, {/@link PyLib#setPythonHome(String)}, and {/@link
 * PyLib#startPython(int, String...)}.
 *
 * <p>
 * Note:
 * <p>
 * We *don't* want JpyConfig to have an explicit dependency on jpy anymore - that way we can still configure jpy without
 * having the unnecessary dependency. For example, the bootstrap kernel needs to be able to configure jpy, but it should
 * not depend on jpy. It's still useful at this time to have fake @links to it though, as it gives useful context for
 * developers. In a better world, the jpy project itself would be better configure-able (ie, not static), and this type
 * of external configuration class wouldn't be necessary.
 */
final public class JpyConfig {
    /**
     * We can't reference the values in {/@link Diag} directly - that would cause {/@link org.jpy.PyLib} to prematurely
     * initialize.
     *
     * We could: 1) Refactor {/@link Diag} so as not to initialize {/@link PyLib} 2) Use compile-time code generation
     * against {/@link Diag} 3) Test to make sure {/@link Flag} and {/@link Diag} are in-sync
     *
     * We are currently doing the #3, see JpyConfigFlagTest
     */
    public enum Flag {
        /**
         * Represents bitset from {/@link Diag#F_OFF}
         */
        OFF(0x00),

        /**
         * Represents bitset from {/@link Diag#F_TYPE}
         */
        TYPE(0x01),

        /**
         * Represents bitset from {/@link Diag#F_METH}
         */
        METH(0x02),

        /**
         * Represents bitset from {/@link Diag#F_EXEC}
         */
        EXEC(0x04),

        /**
         * Represents bitset from {/@link Diag#F_MEM}
         */
        MEM(0x08),

        /**
         * Represents bitset from {/@link Diag#F_JVM}
         */
        JVM(0x10),

        /**
         * Represents bitset from {/@link Diag#F_ERR}
         */
        ERR(0x20),

        /**
         * Represents bitset from {/@link Diag#F_ALL}
         */
        ALL(0XFF);

        public final int bitset;

        Flag(int bitset) {
            this.bitset = bitset;
        }

        public int getBitset() {
            return bitset;
        }
    }

    private final Path pythonHome;
    private final Path programName;
    private final Path pythonLib;
    private final Path jpyLib;
    private final Path jdlLib;
    private final List<Path> extraPaths;
    private final EnumSet<Flag> flags;

    private static void ensureAbsolute(Path path, String name) {
        if (path != null && !path.isAbsolute()) {
            throw new IllegalArgumentException(String.format("%s must be absolute, is '%s'", name, path));
        }
    }

    /**
     *
     * @param programName argument to {/@link PyLib#setProgramName(String)}
     * @param pythonHome argument to {/@link PyLib#setPythonHome(String)}
     * @param pythonLib argument to {/@link PyLibInitializer#initPyLib(String, String, String)}
     * @param jpyLib argument to {/@link PyLibInitializer#initPyLib(String, String, String)}
     * @param jdlLib argument to {/@link PyLibInitializer#initPyLib(String, String, String)}
     * @param extraPaths argument to {/@link PyLib#startPython(int, String...)}
     * @param flags argument to {/@link PyLib#startPython(int, String...)}
     */
    public JpyConfig(
            Path programName,
            Path pythonHome,
            Path pythonLib,
            Path jpyLib,
            Path jdlLib,
            List<Path> extraPaths,
            EnumSet<Flag> flags) {
        ensureAbsolute(programName, "programName");
        ensureAbsolute(pythonHome, "pythonHome");
        ensureAbsolute(pythonLib, "pythonLib");
        ensureAbsolute(jpyLib, "jpyLib");
        ensureAbsolute(jdlLib, "jdlLib");
        if (jpyLib != null
                && jdlLib != null
                && !Objects.equals(jpyLib.getParent(), jdlLib.getParent())) {
            throw new IllegalArgumentException(String.format(
                    "jpy lib and jdl lib must be siblings, jpy is '%s', jdl is '%s', parents '%s' and '%s'",
                    jpyLib, jdlLib, jpyLib.getParent(), jdlLib.getParent()));
        }
        this.programName = programName;
        this.pythonHome = pythonHome;
        this.pythonLib = pythonLib;
        this.jpyLib = jpyLib;
        this.jdlLib = jdlLib;
        this.extraPaths = Objects.requireNonNull(extraPaths);
        for (Path path : extraPaths) {
            Objects.requireNonNull(path);
        }
        this.flags = Objects.requireNonNull(flags);
    }

    public Optional<Path> getPythonHome() {
        return Optional.ofNullable(pythonHome);
    }

    public Optional<Path> getProgramName() {
        return Optional.ofNullable(programName);
    }

    public Optional<Path> getPythonLib() {
        return Optional.ofNullable(pythonLib);
    }

    public Optional<Path> getJpyLib() {
        return Optional.ofNullable(jpyLib);
    }

    public Optional<Path> getJdlLib() {
        return Optional.ofNullable(jdlLib);
    }

    public List<Path> getExtraPaths() {
        return extraPaths;
    }

    public EnumSet<Flag> getFlags() {
        return flags;
    }

    public JpyConfigSource asSource() {
        return new AsSource();
    }

    class AsSource implements JpyConfigSource {

        @Override
        public Optional<String> getFlags() {
            return flags.isEmpty() ? Optional.empty()
                    : Optional.of(flags.stream().map(Enum::name).collect(Collectors.joining(",")));
        }

        @Override
        public Optional<String> getExtraPaths() {
            return extraPaths.isEmpty() ? Optional.empty()
                    : Optional.of(extraPaths.stream().map(Path::toString).collect(Collectors.joining(",")));
        }

        @Override
        public Optional<String> getPythonHome() {
            return Optional.ofNullable(pythonHome).map(Path::toString);
        }

        @Override
        public Optional<String> getProgramName() {
            return Optional.ofNullable(programName).map(Path::toString);
        }

        @Override
        public Optional<String> getPythonLib() {
            return Optional.ofNullable(pythonLib).map(Path::toString);
        }

        @Override
        public Optional<String> getJpyLib() {
            return Optional.ofNullable(jpyLib).map(Path::toString);
        }

        @Override
        public Optional<String> getJdlLib() {
            return Optional.ofNullable(jdlLib).map(Path::toString);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JpyConfig jpyConfig = (JpyConfig) o;

        if (pythonHome != null ? !pythonHome.equals(jpyConfig.pythonHome)
                : jpyConfig.pythonHome != null) {
            return false;
        }
        if (programName != null ? !programName.equals(jpyConfig.programName)
                : jpyConfig.programName != null) {
            return false;
        }
        if (pythonLib != null ? !pythonLib.equals(jpyConfig.pythonLib) : jpyConfig.pythonLib != null) {
            return false;
        }
        if (jpyLib != null ? !jpyLib.equals(jpyConfig.jpyLib) : jpyConfig.jpyLib != null) {
            return false;
        }
        if (jdlLib != null ? !jdlLib.equals(jpyConfig.jdlLib) : jpyConfig.jdlLib != null) {
            return false;
        }
        if (!extraPaths.equals(jpyConfig.extraPaths)) {
            return false;
        }
        return flags.equals(jpyConfig.flags);
    }

    @Override
    public int hashCode() {
        int result = pythonHome != null ? pythonHome.hashCode() : 0;
        result = 31 * result + (programName != null ? programName.hashCode() : 0);
        result = 31 * result + (pythonLib != null ? pythonLib.hashCode() : 0);
        result = 31 * result + (jpyLib != null ? jpyLib.hashCode() : 0);
        result = 31 * result + (jdlLib != null ? jdlLib.hashCode() : 0);
        result = 31 * result + extraPaths.hashCode();
        result = 31 * result + flags.hashCode();
        return result;
    }
}
