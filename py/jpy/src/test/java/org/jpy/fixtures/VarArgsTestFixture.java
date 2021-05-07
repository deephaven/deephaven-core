/*
 * Copyright 2015 Brockmann Consult GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file was modified by Deephaven Data Labs.
 *
 */

package org.jpy.fixtures;

import java.lang.reflect.Array;

/**
 * Used as a test class for the test cases in jpy_overload_test.py
 *
 * @author Norman Fomferra
 */
@SuppressWarnings("UnusedDeclaration")
public class VarArgsTestFixture {

    public String join(String prefix, int ... a) {
        return stringifyArgs(prefix, a);
    }

    public String join(String prefix, double ... a) {
        return stringifyArgs(prefix, a);
    }
    public String join(String prefix, float ... a) {
        return stringifyArgs(prefix, a);
    }
    public String join(String prefix, String ... a) {
        return stringifyArgs(prefix, a);
    }

    public String joinFloat(String prefix, float ... a) {
        return stringifyArgs(prefix, a);
    }

    public String joinLong(String prefix, long ... a) {
        return stringifyArgs(prefix, a);
    }
    public String joinShort(String prefix, short ... a) {
        return stringifyArgs(prefix, a);
    }
    public String joinByte(String prefix, byte ... a) {
        return stringifyArgs(prefix, a);
    }
    public String joinChar(String prefix, char ... a) {
        return stringifyArgs(prefix, a);
    }
    public String joinBoolean(String prefix, boolean ... a) {
        return stringifyArgs(prefix, a);
    }
    public String joinObjects(String prefix, Object ... a) {
        return stringifyArgs(prefix, a);
    }

    public int chooseFixedArity(int... a) {
	    return 2;
    }

    public int chooseFixedArity() {
	    return 1;
    }

    public int stringOrObjectVarArgs(String ... a) {
        return 1 + a.length;
    }
    public int stringOrObjectVarArgs(Object ... a) {
        return 2 + a.length;
    }

    static String stringifyArgs(Object... args) {
        StringBuilder argString = new StringBuilder();
        for (int i = 0; i < args.length; i++) {
            if (i > 0) {
                argString.append(",");
            }
            Object arg = args[i];
            if (arg != null) {
                Class<?> argClass = arg.getClass();
                argString.append(argClass.getSimpleName());
                argString.append('(');
                if (argClass.isArray()) {
                    stringifyArray(arg, argString);
                } else {
                    stringifyObject(arg, argString);
                }
                argString.append(')');
            } else {
                argString.append("null");
            }
        }
        return argString.toString();
    }

    private static void stringifyObject(Object arg, StringBuilder argString) {
        argString.append(String.valueOf(arg));
    }

    private static void stringifyArray(Object arg, StringBuilder argString) {
        boolean primitive = arg.getClass().getComponentType().isPrimitive();
        int length = Array.getLength(arg);
        for (int i = 0; i < length; i++) {
            Object item = Array.get(arg, i);
            if (i > 0) {
                argString.append(",");
            }
            if (primitive) {
                argString.append(String.valueOf(item));
            } else {
                argString.append(stringifyArgs(item));
            }
        }
    }


}
