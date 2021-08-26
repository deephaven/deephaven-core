package io.deephaven.python;

import com.google.common.io.Resources;
import org.jpy.CreateModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.jpy.PyObject;

/**
 * Allows us to create a {@link PyObject} from a resource.
 *
 * This sort of functionality is needed because python is unable to natively find java resource modules, since java
 * resources aren't exposed to the filesystem.
 *
 * Note: we could implement a custom module loader, via https://www.python.org/dev/peps/pep-0302/, if we wanted to
 * resolve java resources more natively.
 */
public class PyModuleFromResource {
    public static PyObject load(Class<?> clazz, String moduleName, String resource) {
        final String contents;
        try {
            contents = Resources.toString(
                    Resources.getResource(clazz, resource),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (final CreateModule creator = CreateModule.create()) {
            return creator.call(moduleName, contents);
        }
    }
}
