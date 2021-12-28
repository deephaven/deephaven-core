package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationConfig;
import io.deephaven.appmode.ApplicationConfigImpl;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class ApplicationConfigTest {

    @Test
    public void clazz() throws IOException, ClassNotFoundException {
        check("00-static.app", ApplicationConfigs.app00());
    }

    @Test
    public void groovy() throws IOException, ClassNotFoundException {
        check("01-groovy.app", ApplicationConfigs.app01());
    }

    @Test
    public void python() throws IOException, ClassNotFoundException {
        check("02-python.app", ApplicationConfigs.app02());
    }

    @Test
    public void qst() throws IOException, ClassNotFoundException {
        check("03-qst.app", ApplicationConfigs.app03());
    }

    @Test
    public void dynamic() throws IOException, ClassNotFoundException {
        check("04-dynamic.app", ApplicationConfigs.app04());
    }

    @Test
    public void find() throws IOException, ClassNotFoundException {
        Assertions.assertThat(ApplicationConfigImpl.find(ApplicationConfigs.resolve(".")))
                .containsExactly(ApplicationConfigs.app00(), ApplicationConfigs.app01(), ApplicationConfigs.app02(),
                        ApplicationConfigs.app03(), ApplicationConfigs.app04());
    }

    private static void check(String path, ApplicationConfig expected) throws IOException, ClassNotFoundException {
        assertThat(app(path)).isEqualTo(expected);
    }

    private static ApplicationConfig app(String s) throws IOException, ClassNotFoundException {
        return ApplicationConfigImpl.parse(ApplicationConfigs.resolve(s));
    }
}
