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

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * jpy's ScriptEngineFactory implementation of JSR 223: <i>Scripting for the Java Platform</i>.
 * <p>
 * Note, jpy's JSR 223 support is not yet functional. This class is only used by unit-tests so far.
 *
 * @author Norman Fomferra
 */
public class ScriptEngineFactoryImpl implements ScriptEngineFactory {

    private final Map<String, Object> parameters;
    private ScriptEngineImpl scriptEngine;

    public ScriptEngineFactoryImpl() {
        parameters = new HashMap<>();
        parameters.put(ScriptEngine.NAME, getNames().get(0));
        parameters.put(ScriptEngine.ENGINE, getEngineName());
        parameters.put(ScriptEngine.ENGINE_VERSION, getEngineVersion());
        parameters.put(ScriptEngine.LANGUAGE, getLanguageName());
        parameters.put(ScriptEngine.LANGUAGE_VERSION, getLanguageVersion());
    }

    /**
     * Returns the full  name of the <code>ScriptEngine</code>.  For
     * instance an implementation based on the Mozilla Rhino Javascript engine
     * might return <i>Rhino Mozilla Javascript Engine</i>.
     *
     * @return The name of the engine implementation.
     */
    @Override
    public String getEngineName() {
        return "jpy Python Engine";
    }

    /**
     * Returns the version of the <code>ScriptEngine</code>.
     *
     * @return The <code>ScriptEngine</code> implementation version.
     */
    @Override
    public String getEngineVersion() {
        return "0.1-alpha";
    }

    /**
     * Returns an immutable list of filename extensions, which generally identify scripts
     * written in the language supported by this <code>ScriptEngine</code>.
     * The array is used by the <code>ScriptEngineManager</code> to implement its
     * <code>getEngineByExtension</code> method.
     *
     * @return The list of extensions.
     */
    @Override
    public List<String> getExtensions() {
        return Collections.unmodifiableList(Arrays.asList("py", "pyc"));
    }

    /**
     * Returns an immutable list of mimetypes, associated with scripts that
     * can be executed by the engine.  The list is used by the
     * <code>ScriptEngineManager</code> class to implement its
     * <code>getEngineByMimetype</code> method.
     *
     * @return The list of mime types.
     */
    @Override
    public List<String> getMimeTypes() {
        return Collections.unmodifiableList(Arrays.asList("text/python", "application/python", "text/x-python", "application/x-python"));
    }

    /**
     * Returns an immutable list of  short names for the <code>ScriptEngine</code>, which may be used to
     * identify the <code>ScriptEngine</code> by the <code>ScriptEngineManager</code>.
     * For instance, an implementation based on the Mozilla Rhino Javascript engine might
     * return list containing {&quot;javascript&quot;, &quot;rhino&quot;}.
     *
     * @return an immutable list of short names
     */
    @Override
    public List<String> getNames() {
        return Collections.unmodifiableList(Arrays.asList("cpython", "python"));
    }

    /**
     * Returns the name of the scripting langauge supported by this
     * <code>ScriptEngine</code>.
     *
     * @return The name of the supported language.
     */
    @Override
    public String getLanguageName() {
        return "python";
    }

    /**
     * Returns the version of the scripting language supported by this
     * <code>ScriptEngine</code>.
     *
     * @return The version of the supported language.
     */
    @Override
    public String getLanguageVersion() {
        // todo - return true Python version --> e.g. call python -V
        // using PyLibConfig.getProperties().getProperty("jpy.pythonExecutable")
        return "3.x";
    }

    /**
     * Returns the value of an attribute whose meaning may be implementation-specific.
     * Keys for which the value is defined in all implementations are:
     * <ul>
     * <li>ScriptEngine.ENGINE</li>
     * <li>ScriptEngine.ENGINE_VERSION</li>
     * <li>ScriptEngine.NAME</li>
     * <li>ScriptEngine.LANGUAGE</li>
     * <li>ScriptEngine.LANGUAGE_VERSION</li>
     * </ul>
     * <p>
     * The values for these keys are the Strings returned by <code>getEngineName</code>,
     * <code>getEngineVersion</code>, <code>getName</code>, <code>getLanguageName</code> and
     * <code>getLanguageVersion</code> respectively.<br><br>
     * A reserved key, <code><b>THREADING</b></code>, whose value describes the behavior of the engine
     * with respect to concurrent execution of scripts and maintenance of state is also defined.
     * These values for the <code><b>THREADING</b></code> key are:<br><br>
     * <ul>
     * <li><code>null</code> - The engine implementation is not thread safe, and cannot
     * be used to execute scripts concurrently on multiple threads.
     * <li><code>&quot;MULTITHREADED&quot;</code> - The engine implementation is internally
     * thread-safe and scripts may execute concurrently although effects of script execution
     * on one thread may be visible to scripts on other threads.
     * <li><code>&quot;THREAD-ISOLATED&quot;</code> - The implementation satisfies the requirements
     * of &quot;MULTITHREADED&quot;, and also, the engine maintains independent values
     * for symbols in scripts executing on different threads.
     * <li><code>&quot;STATELESS&quot;</code> - The implementation satisfies the requirements of
     * <li><code>&quot;THREAD-ISOLATED&quot;</code>.  In addition, script executions do not alter the
     * mappings in the <code>Bindings</code> which is the engine scope of the
     * <code>ScriptEngine</code>.  In particular, the keys in the <code>Bindings</code>
     * and their associated values are the same before and after the execution of the script.
     * </ul>
     * <br><br>
     * Implementations may define implementation-specific keys.
     *
     * @param key The name of the parameter
     * @return The value for the given parameter. Returns <code>null</code> if no
     * value is assigned to the key.
     */
    @Override
    public Object getParameter(String key) {
        return parameters.get(key);
    }

    /**
     * Returns a String which can be used to invoke a method of a  Java object using the syntax
     * of the supported scripting language.  The method returns
     * <pre>
     *  {@code String.format("%s.%s(%s)", obj, m, String.join(", ", args)); }
     * </pre>
     *
     * @param obj  The name representing the object whose method is to be invoked. The
     *             name is the one used to create bindings using the <code>put</code> method of
     *             <code>ScriptEngine</code>, the <code>put</code> method of an <code>ENGINE_SCOPE</code>
     *             <code>Bindings</code>,or the <code>setAttribute</code> method
     *             of <code>ScriptContext</code>.  The identifier used in scripts may be a decorated form of the
     *             specified one.
     * @param m    The name of the method to invoke.
     * @param args names of the arguments in the method call.
     * @return The String used to invoke the method in the syntax of the scripting language.
     */
    @Override
    public String getMethodCallSyntax(String obj, String m, String... args) {
        return String.format("%s.%s(%s)", obj, m, String.join(", ", args));
    }

    /**
     * Returns a String that can be used as a statement to display the specified String  using
     * the syntax of the supported scripting language.  The method returns
     * <pre>
     *  {@code "print(" + toDisplay + ")";}
     * </pre>
     *
     * @param toDisplay The String to be displayed by the returned statement.
     * @return The string used to display the String in the syntax of the scripting language.
     */
    @Override
    public String getOutputStatement(String toDisplay) {
        return "print(" + toDisplay + ")";
    }

    /**
     * Returns a valid scripting language executable program with given statements.
     * The method returns
     * <pre>
     *  {@code String.join("\n", statements);}
     * </pre>
     *
     * @param statements The statements to be executed.  May be return values of
     *                   calls to the <code>getMethodCallSyntax</code> and <code>getOutputStatement</code> methods.
     * @return The Program
     */
    @Override
    public String getProgram(String... statements) {
        return String.join("\n", statements);
    }

    /**
     * Returns an instance of the <code>ScriptEngine</code> associated with this
     * <code>ScriptEngineFactory</code>. A new ScriptEngine is generally
     * returned, but implementations may pool, share or reuse engines.
     *
     * @return A new <code>ScriptEngine</code> instance.
     */
    @Override
    public ScriptEngine getScriptEngine() {
        if (scriptEngine == null) {
            synchronized (this) {
                if (scriptEngine == null) {
                    scriptEngine = new ScriptEngineImpl(this);
                }
            }
        }
        return scriptEngine;
    }
}
