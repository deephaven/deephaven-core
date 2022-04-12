package io.deephaven.server.appmode;

import com.google.rpc.Code;
import io.deephaven.appmode.ApplicationConfig;
import io.deephaven.appmode.ApplicationContext;
import io.deephaven.appmode.ApplicationState;
import io.deephaven.appmode.DynamicApplication;
import io.deephaven.appmode.QSTApplication;
import io.deephaven.appmode.ScriptApplication;
import io.deephaven.appmode.StaticClassApplication;
import io.deephaven.engine.util.GroovyDeephavenSession;
import io.deephaven.engine.util.PythonDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.extensions.barrage.util.GrpcUtil;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class ApplicationFactory implements ApplicationConfig.Visitor {

    public static ApplicationState create(Path applicationDir, ApplicationConfig config, ScriptSession scriptSession,
            ApplicationState.Listener appStateListener) {
        return config.walk(new ApplicationFactory(applicationDir, scriptSession, appStateListener)).out();
    }

    private final Path applicationDir;
    private final ScriptSession scriptSession;
    private final ApplicationState.Listener appStateListener;

    private ApplicationState out;

    private ApplicationFactory(final Path applicationDir,
            final ScriptSession scriptSession,
            final ApplicationState.Listener appStateListener) {
        this.applicationDir = Objects.requireNonNull(applicationDir);
        this.scriptSession = scriptSession;
        this.appStateListener = appStateListener;
    }

    public ApplicationState out() {
        return Objects.requireNonNull(out);
    }

    @Override
    public void visit(ScriptApplication app) {
        if (app.scriptType().equals("groovy")) {
            if (!(scriptSession instanceof GroovyDeephavenSession)) {
                throw new IllegalArgumentException(String.format(
                        "Cannot instantiate Groovy application on a %s app session", scriptSession.scriptType()));
            }
        } else if (app.scriptType().equals("python")) {
            if (!(scriptSession instanceof PythonDeephavenSession)) {
                throw new IllegalArgumentException(String.format(
                        "Cannot instantiate Python application on a %s app session", scriptSession.scriptType()));
            }
        } else {
            throw new UnsupportedOperationException(String.format(
                    "Currently do not support script sessions of type '%s'", app.scriptType()));
        }

        out = new ScriptApplicationState(scriptSession, appStateListener, app.id(), app.name());
        ApplicationContext.runUnderContext(out, () -> evaluateScripts(app.files()));
    }

    @Override
    public void visit(DynamicApplication<?> advanced) {
        try {
            out = advanced.create(appStateListener);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void visit(QSTApplication qst) {
        throw GrpcUtil.statusRuntimeException(Code.UNIMPLEMENTED, "See deephaven-core#1080; support qst application");
    }

    @Override
    public void visit(StaticClassApplication<?> clazz) {
        try {
            out = clazz.create().toState(appStateListener);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private Path absolutePath(Path path) {
        return path.isAbsolute() ? path : applicationDir.resolve(path);
    }

    private void evaluateScripts(List<Path> files) {
        for (Path file : files) {
            scriptSession.evaluateScript(absolutePath(file));
        }
    }
}
