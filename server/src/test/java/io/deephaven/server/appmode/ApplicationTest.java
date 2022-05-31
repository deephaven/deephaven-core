package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.Field;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.AbstractScriptSession;
import io.deephaven.engine.util.GroovyDeephavenSession;
import io.deephaven.engine.util.PythonDeephavenSession;
import io.deephaven.plugin.type.ObjectTypeLookup.NoOp;
import io.deephaven.test.junit4.EngineCleanup;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

public class ApplicationTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private AbstractScriptSession session = null;

    @After
    public void tearDown() {
        if (session != null) {
            session.release();
            session = null;
        }
    }

    @Test
    public void app00() {
        ApplicationState app = ApplicationFactory.create(ApplicationConfigs.testAppDir(), ApplicationConfigs.app00(),
                session, new NoopStateListener());
        assertThat(app.name()).isEqualTo("My Class Application");
        assertThat(app.numFieldsExported()).isEqualTo(2);
        assertThat(app.getField("hello").value()).isInstanceOf(Table.class);
        assertThat(app.getField("world").value()).isInstanceOf(Table.class);
    }

    @Test
    public void app01() throws IOException {
        session = new GroovyDeephavenSession(NoOp.INSTANCE, null, GroovyDeephavenSession.RunScripts.none(), false);
        ApplicationState app = ApplicationFactory.create(ApplicationConfigs.testAppDir(), ApplicationConfigs.app01(),
                session, new NoopStateListener());
        assertThat(app.name()).isEqualTo("My Groovy Application");
        assertThat(app.numFieldsExported()).isEqualTo(2);
        assertThat(app.getField("hello").value()).isInstanceOf(Table.class);
        assertThat(app.getField("world").value()).isInstanceOf(Table.class);
    }

    @Test
    @Ignore("TODO: deephaven-core#1741 python test needs to run in a container")
    public void app02() throws IOException, InterruptedException, TimeoutException {
        session = new PythonDeephavenSession(NoOp.INSTANCE, null, false, false);
        ApplicationState app = ApplicationFactory.create(ApplicationConfigs.testAppDir(), ApplicationConfigs.app02(),
                session, new NoopStateListener());
        assertThat(app.name()).isEqualTo("My Python Application");
        assertThat(app.numFieldsExported()).isEqualTo(2);
        assertThat(app.getField("hello").value()).isInstanceOf(Table.class);
        assertThat(app.getField("world").value()).isInstanceOf(Table.class);
    }

    @Test
    @Ignore("TODO: deephaven-core#1080 support QST application")
    public void app03() {
        ApplicationState app = ApplicationFactory.create(ApplicationConfigs.testAppDir(), ApplicationConfigs.app03(),
                session, new NoopStateListener());
        assertThat(app.name()).isEqualTo("My QST Application");
        assertThat(app.numFieldsExported()).isEqualTo(2);
        assertThat(app.getField("hello").value()).isInstanceOf(Table.class);
        assertThat(app.getField("world").value()).isInstanceOf(Table.class);
    }

    @Test
    public void app04() {
        ApplicationState app = ApplicationFactory.create(ApplicationConfigs.testAppDir(), ApplicationConfigs.app04(),
                session, new NoopStateListener());
        assertThat(app.name()).isEqualTo("My Dynamic Application");
    }

    private static class NoopStateListener implements ApplicationState.Listener {
        @Override
        public void onNewField(ApplicationState app, Field<?> field) {
            // ignore
        }

        @Override
        public void onRemoveField(ApplicationState app, Field<?> field) {
            // ignore
        }
    }
}
