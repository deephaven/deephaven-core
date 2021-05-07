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

import java.io.IOException;

/**
 * Used as a test class for the test cases in jpy_exception_test.py
 *
 * @author Norman Fomferra
 */
@SuppressWarnings("UnusedDeclaration")
public class ExceptionTestFixture {
    public int throwNpeIfArgIsNull(String arg) {
        return arg.length();
    }

    public int throwNpeIfArgIsNull2(String arg) {
        return throwNpeIfArgIsNull(arg);
    }

    public int throwNpeIfArgIsNullNested(String arg) {
        try {
            return throwNpeIfArgIsNull(arg);
	    } catch (Exception e) {
    		throw new RuntimeException("Nested exception", e);
	    }
    }

    public int throwNpeIfArgIsNullNested2(String arg) {
        return throwNpeIfArgIsNullNested(arg);
    }

    public int throwNpeIfArgIsNullNested3(String arg) {
        try {
            return throwNpeIfArgIsNullNested2(arg);
	    } catch (Exception e) {
    		throw new RuntimeException("Nested exception 3", e);
	    }
    }

    public int throwAioobeIfIndexIsNotZero(int index) {
        int[] ints = new int[]{101};
        return ints[index];
    }


    public void throwRteIfMessageIsNotNull(String message) {
        if (message != null) {
            throw new RuntimeException(message);
        }
    }

    public void throwIoeIfMessageIsNotNull(String message) throws IOException {
        if (message != null) {
            throw new IOException(message);
        }
    }

}
