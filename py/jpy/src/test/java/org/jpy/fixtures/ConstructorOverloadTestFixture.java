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
 * @author Norman Fomferra
 */
@SuppressWarnings("UnusedDeclaration")
public class ConstructorOverloadTestFixture {
    String state;

    public ConstructorOverloadTestFixture() {
        initState();
    }

    public ConstructorOverloadTestFixture(int a) {
        initState(a);
    }

    public ConstructorOverloadTestFixture(int a, int b) {
        initState(a, b);
    }

    public ConstructorOverloadTestFixture(float a) {
        initState(a);
    }

    public ConstructorOverloadTestFixture(float a, float b) {
        initState(a, b);
    }

    public ConstructorOverloadTestFixture(int a, float b) {
        initState(a, b);
    }

    public ConstructorOverloadTestFixture(float a, int b) {
        initState(a, b);
    }

    public String getState() {
        return state;
    }

    private void initState(Object... args) {
        state = MethodOverloadTestFixture.stringifyArgs(args);
    }
}
