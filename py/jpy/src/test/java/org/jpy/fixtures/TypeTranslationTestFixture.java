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

/**
 * Used as a test class for the test cases in jpy_retval_test.py
 * Note: Please make sure to not add any method overloads to this class.
 * This is done in {@link MethodOverloadTestFixture}.
 *
 * @author Norman Fomferra
 */
@SuppressWarnings("UnusedDeclaration")
public class TypeTranslationTestFixture {
    public Thing makeThing(int value) {
        return new Thing(value);
    }
}
