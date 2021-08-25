package io.deephaven.db.util;

import org.jpy.PyInputMode;
import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;

import java.util.List;
import java.util.Map;

/**
 * Playpen for Jpy integration.
 */
public class JpyPlaypen {

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("jpy.jdlLib", "/Library/Python/2.7/site-packages/jdl.so");
        System.setProperty("jpy.pythonLib",
                "/System/Library/Frameworks/Python.framework/Versions/2.7/lib/libpython2.7.dylib");
        System.setProperty("jpy.jpyLib", "/Library/Python/2.7/site-packages/jpy.so");
        PyLib.startPython();

        PyModule main = PyModule.getMain();

        main.setAttribute("yo", 2);
        main.executeCode("print yo\n", PyInputMode.SCRIPT);

        Thread.sleep(1000);

        PyObject s = main.executeCode("s = 'Hello World'\nprint s\nii = 7\njj = 8.0\n", PyInputMode.SCRIPT);


        if (s != null) {
            System.out.println("S = \"" + s + "\" (" + s.getClass().getCanonicalName() + ")");
            Object objectValue = s.getObjectValue();
            System.out.println("OV = \"" + objectValue + "\" ("
                    + (objectValue != null ? objectValue.getClass().getCanonicalName() : "(null)") + ")");
            PyObject type = s.getAttribute("__class__");
            System.out.println("Type = \"" + type);
        } else {
            System.out.println("S = null");
        }

        s = main.executeCode("s", PyInputMode.EXPRESSION);
        if (s != null) {
            System.out.println("S = \"" + s + "\" (" + s.getClass().getCanonicalName() + ")");
            Object objectValue = s.getObjectValue();
            System.out.println("OV = \"" + objectValue + "\" ("
                    + (objectValue != null ? objectValue.getClass().getCanonicalName() : "(null)") + ")");
            PyObject type = s.getAttribute("__class__");
            System.out.println("Type = \"" + type);
        } else {
            System.out.println("S = null");
        }

        PyObject num = main.executeCode("ii", PyInputMode.EXPRESSION);
        if (num != null) {
            System.out.println("II = \"" + num + "\" (" + num.getClass().getCanonicalName() + ")");
            Object objectValue = num.getObjectValue();
            System.out.println("OV = \"" + objectValue + "\" ("
                    + (objectValue != null ? objectValue.getClass().getCanonicalName() : "(null)") + ")");
        } else {
            System.out.println("S = null");
        }

        num = main.executeCode("jj", PyInputMode.EXPRESSION);
        if (num != null) {
            System.out.println("JJ = \"" + num + "\" (" + num.getClass().getCanonicalName() + ")");
            Object objectValue = num.getObjectValue();
            System.out.println("OV = \"" + objectValue + "\" ("
                    + (objectValue != null ? objectValue.getClass().getCanonicalName() : "(null)") + ")");
        } else {
            System.out.println("S = null");
        }

        s = main.executeCode("globals().keys()", PyInputMode.EXPRESSION);
        if (s != null) {
            System.out.println("Globals().keys() = \"" + s + "\" (" + num.getClass().getCanonicalName() + ")");
            Object objectValue = s.getType();
            System.out.println("OV = \"" + objectValue + "\" ("
                    + (objectValue != null ? objectValue.getClass().getCanonicalName() : "(null)") + ")");
            if (s.isList()) {
                System.out.println("isList()");
                List<PyObject> asList = s.asList();
                System.out.println(asList);
                System.out.println(asList.size());
                for (PyObject pyObject : asList) {
                    System.out.println("LI: " + pyObject.getObjectValue());
                }
            }
        } else {
            System.out.println("S = null");
        }

        s = main.executeCode("globals()", PyInputMode.EXPRESSION);
        if (s != null) {
            System.out.println("Globals() = \"" + s + "\" (" + num.getClass().getCanonicalName() + ")");
            PyObject type = s.getType();
            System.out.println(
                    "Type = \"" + type + "\" (" + (type != null ? type.getClass().getCanonicalName() : "(null)") + ")");

            System.out.println("Dict: " + s.isDict());

            if (s.isDict()) {
                System.out.println("isDict");
                Map<PyObject, PyObject> dict = s.asDict();
                for (Map.Entry<PyObject, PyObject> kv : dict.entrySet()) {
                    System.out.println("{" + kv.getKey() + ", " + kv.getValue() + "}");
                }
            }
        } else {
            System.out.println("S = null");
        }

        PyObject pyObject = main.executeCode("quux", PyInputMode.EXPRESSION);
        System.out.println(pyObject);

        System.out.println("Getting main!");

        PyLib.stopPython();


        // jep.evalStatement("print \"Line 1\"\nprint\"Line 2\"");

        // jep.evalStatement("from java.lang import System");
        // jep.evalStatement("s = 'Hello World'");
        // jep.evalStatement("System.out.println(s)");
        // jep.evalStatement("print(s)");
        // jep.evalStatement("print(s[1:-1])");
        //
        // jep.set("x", 1);
        // jep.evalStatement("print(x)");
        // jep.evalStatement("x += 1");
        // System.out.println("X: " + jep.getValue("x"));
        //
        //
        // jep.evalStatement("from io.deephaven.db.util import JepDummy");
        // JepDummy q = new JepDummy(7);
        // jep.set("q", q);
        //
        // jep.evalStatement("print(q)");
        // jep.evalStatement("print(type(q))");
        // jep.evalStatement("print(dir(q))");
        // jep.evalStatement("print q.java_name");
        // jep.evalStatement("q2 = JepDummy(3)");
        // jep.evalStatement("print(q2)");
        //
        // jep.evalStatement("q.increment()");
        // jep.evalStatement("print(q.get())");
        //
        // System.out.println(q.get());
        //
        // JepDummy q2 = (JepDummy)jep.getValue("q2");
        // System.out.println(q2);
        // q2.increment();
        // System.out.println(q2.get());
        //
        // jep.evalStatement("print(globals())");Evaluating friends.
        // Object value = jep.getValue("globals()");
        // System.out.println(value);

        // System.out.println("Evaluating friends.");
        //
        // String s1 = "friends = ['john', 'pat', 'gary', 'michael']\n";
        // String s2 = "for i, name in enumerate(friends):\n";
        // String s3 = " print \"iteration {iteration} is {name}\".format(iteration=i, name=name)";
        // boolean x1 = jep.evalStatement(s1 + s2 + s3);
        //
        // System.out.println("Done Evaluating friends: " + x1);

        // System.out.println(jep.getValue("globals()"));
        //
        // jep.close();
    }
}
