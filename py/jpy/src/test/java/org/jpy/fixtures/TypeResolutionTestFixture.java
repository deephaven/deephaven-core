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
 */

package org.jpy.fixtures;

/**
 * Used as a test class for the test cases in jpy_typeres_test.py
 *
 * @author Norman Fomferra
 */
@SuppressWarnings("UnusedDeclaration")
public class TypeResolutionTestFixture {

    public SuperThing createSuperThing(int value) {
        return new SuperThing(value);
    }


    public static class SuperThing extends Thing {
        public SuperThing(int value) {
            super(value);
        }

        public void add(int val) {
            setValue(getValue() + val);
        }
    }
}
