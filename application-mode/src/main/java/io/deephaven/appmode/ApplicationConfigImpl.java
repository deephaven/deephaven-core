package io.deephaven.appmode;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

public class ApplicationConfigImpl {

    public static final String APPLICATION_DIR_PROP = "deephaven.application.dir";

    public static final String APPLICATION_DIR = System.getProperty(APPLICATION_DIR_PROP, null);

    public static List<ApplicationConfig> find(Path dir) throws IOException, ClassNotFoundException {
        try (Stream<Path> stream =
                Files.list(dir).filter(ApplicationConfigImpl::isAppFile).sorted()) {
            List<ApplicationConfig> configs = new ArrayList<>();
            Iterator<Path> it = stream.iterator();
            while (it.hasNext()) {
                configs.add(parse(it.next()));
            }
            return configs;
        }
    }

    private static boolean isAppFile(Path path) {
        return path.getFileName().toString().endsWith(".app") && Files.isReadable(path)
                && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS);
    }

    public static ApplicationConfig parse(Path file) throws IOException, ClassNotFoundException {
        Properties properties = new Properties();
        try (BufferedReader reader = Files.newBufferedReader(file)) {
            properties.load(reader);
        }
        String type = properties.getProperty("type");
        if (type == null) {
            throw new IllegalArgumentException("Application property 'type' not specified");
        }
        switch (type) {
            case QSTApplication.TYPE:
                return QSTApplication.parse(properties);
            case ScriptApplication.TYPE:
                return ScriptApplication.parse(properties);
            case StaticClassApplication.TYPE:
                return StaticClassApplication.parse(properties);
            case DynamicApplication.TYPE:
                return DynamicApplication.parse(properties);
        }
        throw new IllegalArgumentException("Unexpected type " + type);
    }
}
