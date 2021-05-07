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

package org.jpy.jsr223;

import org.junit.Assert;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class Jsr223Test {

    @Test
    public void testThatScriptEngineFactoryIsRegistered() throws Exception {
        assertNotNull(getScriptEngineFactory());
    }

    @Test
    public void testThatScriptEngineFactorySupportsMinimumParameterKeys() throws Exception {
        ScriptEngineFactoryImpl scriptEngineFactory = getScriptEngineFactory();
        assertEquals("cpython", scriptEngineFactory.getParameter(ScriptEngine.NAME));
        assertEquals("jpy Python Engine", scriptEngineFactory.getParameter(ScriptEngine.ENGINE));
        assertEquals("0.1-alpha", scriptEngineFactory.getParameter(ScriptEngine.ENGINE_VERSION));
        assertEquals("python", scriptEngineFactory.getParameter(ScriptEngine.LANGUAGE));
        assertEquals("3.x", scriptEngineFactory.getParameter(ScriptEngine.LANGUAGE_VERSION));
    }

    private ScriptEngineFactoryImpl getScriptEngineFactory() {
        ScriptEngineManager engineManager = new ScriptEngineManager();
        List<ScriptEngineFactory> engineFactories = engineManager.getEngineFactories();
        ScriptEngineFactoryImpl engineFactoryImpl = null;
        for (ScriptEngineFactory engineFactory : engineFactories) {
            if (engineFactory instanceof ScriptEngineFactoryImpl) {
                engineFactoryImpl = (ScriptEngineFactoryImpl) engineFactory;
            }
        }
        return engineFactoryImpl;
    }
}
