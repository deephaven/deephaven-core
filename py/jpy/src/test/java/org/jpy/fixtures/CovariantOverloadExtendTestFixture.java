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
 */

package org.jpy.fixtures;

import java.lang.reflect.Array;
import java.lang.reflect.Method;

/**
 * Used as a test class for the test cases in jpy_overload_test.py
 *
 * @author Charles P. Wright
 */
@SuppressWarnings("UnusedDeclaration")
public class CovariantOverloadExtendTestFixture extends CovariantOverloadTestFixture {
    public CovariantOverloadExtendTestFixture(int x) {
        super(x * 2);
    }

    public CovariantOverloadExtendTestFixture foo(Number a, int b) {
        return new CovariantOverloadExtendTestFixture(a.intValue() - b);
    }
}
