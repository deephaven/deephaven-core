package io.deephaven.db.util;

import scala.Option;
import scala.tools.nsc.interpreter.IMain;

import javax.script.*;
import java.util.Collections;
import java.util.List;

public class ScriptEnginePlaypen {
    public static void main(String[] args) throws ScriptException {
        System.setProperty("scala.usejavacp", "true");
        ScriptEngineManager factory = new ScriptEngineManager();

        for (ScriptEngineFactory sef : factory.getEngineFactories()) {
            System.out.println(sef.getEngineName() + ", " + sef.getLanguageName() + ", " + sef.getNames());
        }

        ScriptEngine engine = factory.getEngineByName("scala");

        if (engine instanceof IMain) {
            scala.collection.immutable.List<String> emptyList = scala.collection.JavaConverters
                    .collectionAsScalaIterable((List<String>) Collections.EMPTY_LIST).toList();
            ((IMain) engine).bind("z", "Int", 5, emptyList);
            ((IMain) engine).bind("y", "Int", 6, emptyList);
        } else {
            // engine.put("y: Int", 5);
            // engine.put("z: Int", 6);
        }

        System.out.println("w: " + engine.get("w"));
        System.out.println("x: " + engine.get("x"));
        System.out.println("y: " + engine.get("y"));
        System.out.println("z: " + engine.get("z"));

        // doIt(engine, "println(y * 2);");
        // doIt(engine, "math.pow(z, 2)");
        doIt(engine, "var x : Double = math.pow(z, 2); var w : Double = math.pow(2, z);");

        System.out.println("w: " + engine.get("w"));
        System.out.println("x: " + engine.get("x"));
        System.out.println("y: " + engine.get("y"));
        System.out.println("z: " + engine.get("z"));

        if (engine instanceof IMain) {
            Option<Object> x = ((IMain) engine).valueOfTerm("x");
            System.out.println(x.isDefined() ? x.get() : "undef");
            Option<Object> w = ((IMain) engine).valueOfTerm("w");
            System.out.println(w.isDefined() ? w.get() : "undef");
        }
    }

    private static void doIt(ScriptEngine engine, String script) throws ScriptException {
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        System.out.println("bindings: " + bindings.entrySet());
        System.out.println("Eval: " + script);
        Object res = engine.eval(script, bindings);
        System.out.println("res: " + res);
        System.out.println("res.getClass(): " + res.getClass());
        System.out.println("bindings: " + bindings);
    }
}
