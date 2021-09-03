package io.deephaven.engine.util;

import scala.Option;
import scala.collection.immutable.List;
import scala.reflect.internal.Names;
import scala.reflect.internal.Symbols;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;
import scala.tools.nsc.GenericRunnerSettings;
import scala.tools.nsc.interpreter.IMain;
import scala.tools.nsc.interpreter.NamedParamClass;
import scala.tools.nsc.interpreter.Results;
import scala.tools.nsc.settings.MutableSettings;

import javax.script.ScriptException;

/**
 * Try importing the scala library and doing an interpretation of a line of text.
 */
public class ScalaInterpreterPlaypen {


    private static class ErrorHandler extends AbstractFunction1<String, BoxedUnit> {

        @Override


        public BoxedUnit apply(String message) {
            System.err.println("Interpreter error: " + message);
            return BoxedUnit.UNIT;

        }

    }


    public static void main(String[] args) throws ScriptException {
        System.out.println("hi!");

        GenericRunnerSettings settings = new GenericRunnerSettings(new ErrorHandler());
        ((MutableSettings.BooleanSetting) settings.usejavacp()).v_$eq(true);
        IMain interpreter = new IMain(settings);

        interpreter.bind(new NamedParamClass("q", "Double", 7.0));

        Results.Result
                x = interpreter.interpret("var y : Double = scala.math.pow(2, q); var z : Double = y % 2;");
        System.out.println(x);
        System.out.println(x.getClass());

        System.out.println(interpreter.definedSymbolList());
        System.out.println(interpreter.allDefinedNames());
        List < Names.TermName > x1 = interpreter.definedTerms();
        System.out.println(x1);
//        System.out.println("classOfTerm: " + interpreter.classOfTerm("y"));
        Option < Object > y = interpreter.valueOfTerm("y");
        System.out.println("valueOfTerm: " + y);
        if (y.isDefined()) {
            Object yg = y.get();
            System.out.println("valueOfTerm.get(): " + yg);

        }
        Symbols.Symbol head = interpreter.definedSymbolList().head();
        System.out.println(head.typeOfThis());
        System.out.println(head.isInitialized());
        Symbols.Symbol accessed = head.accessed();
        System.out.println(accessed);

        System.exit(0);

    }
}