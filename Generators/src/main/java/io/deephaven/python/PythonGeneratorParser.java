/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.python;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.db.util.string.StringUtils;
import io.deephaven.libs.GroovyStaticImportGenerator;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;


/**
 * Helper class for parsing generator arguments.
 */
class PythonGeneratorParser {

    /**
     * Validate the arguments passed in and parse the arguments for generation.
     * 
     * @param args arguments for gradle task.
     * @param log logger for output.
     */
    static GeneratorElement[] validateArgs(final String[] args, final Logger log) {
        if (args.length <= 2) {
            log.info("No classes to generate.");
            System.exit(0);
        }
        return parse(args, args[0]);
    }

    static void logGenerationMessage(final Logger log, final String javaClass, final String preambleFilePath,
            final String destinationFilePath) {
        log.info("Generating python methods for java class: " + javaClass);
        if (preambleFilePath != null) {
            log.info("Python preamble file: " + preambleFilePath);
        }
        log.info("Output file: " + destinationFilePath);
    }

    /**
     * Parse the provided inputs. It is assumed that args[0] and args[1] should be ignored.
     * 
     * @param args input parameters.
     * @return parsed results.
     */
    static GeneratorElement[] parse(final String[] args, final String devroot) {
        final ArrayList<GeneratorElement> temp = new ArrayList<GeneratorElement>();

        int currentArgument = 2;
        while (currentArgument < args.length) {
            final int nextArgument;
            final String javaClass = args[currentArgument];
            final String preambleFilePath;
            final String destinationFile;
            if (args[currentArgument + 1].endsWith(".py")) {
                preambleFilePath = null;
                destinationFile = devroot + args[currentArgument + 1];
                nextArgument = currentArgument + 2;
            } else {
                preambleFilePath = devroot + args[currentArgument + 1];
                destinationFile = devroot + args[currentArgument + 2];
                nextArgument = currentArgument + 3;
            }
            temp.add(new GeneratorElement(javaClass, preambleFilePath, destinationFile));
            currentArgument = nextArgument;
        }

        GeneratorElement[] out = new GeneratorElement[temp.size()];
        out = temp.toArray(out);
        return out;
    }

    /**
     * Get an array of valid methods we want to consider for a given java class.
     * 
     * @param javaClass fully qualified class name as string.
     * @param filter a predicate to filter out interesting methods from the java class.
     * @return array of valid methods.
     * @throws ClassNotFoundException
     */
    static GroovyStaticImportGenerator.JavaFunction[] getValidMethods(String javaClass, Predicate<Method> filter)
            throws ClassNotFoundException {
        final Class clazz = Class.forName(javaClass);
        final Method[] methods = clazz.getMethods();
        final GroovyStaticImportGenerator.JavaFunction[] sortedMethods = Arrays.stream(methods)
                .filter(filter)
                .map(GroovyStaticImportGenerator.JavaFunction::new)
                .sorted()
                .toArray(GroovyStaticImportGenerator.JavaFunction[]::new);
        return sortedMethods;
    }

    /**
     * Get an array of the public, static methods for a given java class.
     * 
     * @param javaClass fully qualified class string.
     * @param skipGeneration a list of methods to skip of the form "javaClass,methodName".
     * @return array of public, static methods.
     * @throws ClassNotFoundException
     */
    static GroovyStaticImportGenerator.JavaFunction[] getPublicStaticMethods(final String javaClass,
            final List<String> skipGeneration) throws ClassNotFoundException {
        return getValidMethods(javaClass, (m) -> PythonGeneratorParser.isValidPublicStatic(m)
                && (skipGeneration == null || !skipGeneration.contains(javaClass + "," + m.getName())));
    }

    /**
     * Get a details for the public, static methods of the given java class.
     * 
     * @param javaClass fully qualified class string.
     * @param skipGeneration a list of methods to skip of the form "javaClass,methodName".
     * @return MethodContainer corresponding to public, static methods of javaClass.
     * @throws ClassNotFoundException
     */
    static MethodContainer getPublicStaticMethodsDetails(final String javaClass, final List<String> skipGeneration)
            throws ClassNotFoundException {
        return new MethodContainer(getPublicStaticMethods(javaClass, skipGeneration));
    }

    /**
     * Get an array of the public, static methods for a given java class.
     * 
     * @param javaClass fully qualified class string.
     * @return array of public, static methods.
     * @throws ClassNotFoundException
     */
    static GroovyStaticImportGenerator.JavaFunction[] getPublicMethods(String javaClass) throws ClassNotFoundException {
        return getValidMethods(javaClass, PythonGeneratorParser::isValidPublic);
    }

    /**
     * Get a details for the public methods of the given java class.
     * 
     * @param javaClass fully qualified class string.
     * @return MethodContainer corresponding to public methods of javaClass.
     * @throws ClassNotFoundException
     */
    static MethodContainer getPublicMethodsDetails(String javaClass) throws ClassNotFoundException {
        return new MethodContainer(getPublicMethods(javaClass));
    }

    /**
     * @param m The method to filter.
     * @return true if the method is public, static and not from java.lang.Object.
     */
    private static boolean isValidPublicStatic(Method m) {
        return isValid(m) && Modifier.isStatic(m.getModifiers()) && Modifier.isPublic(m.getModifiers());
    }

    /**
     * @param m The method to filter.
     * @return true if the method is public, static and not from java.lang.Object.
     */
    private static boolean isValidPublic(Method m) {
        // Note that this should _probably_ have !Modifier.isStatic(m.getModifiers()),
        // if it being used in tandem with isValidPublicStatic.
        // i.e. this method should _maybe_ be called isValidPublicInstance. SME should make this decision.
        return isValid(m) && Modifier.isPublic(m.getModifiers());
    }

    /**
     * @param m The method to filter.
     * @return true if the method is not from java.lang.Object.
     */
    private static boolean isValid(Method m) {
        return !m.getDeclaringClass().getName().equals(Object.class.getName());
    }

    /**
     * Given generated code string, finalize the generation process.
     * 
     * @param code generated code.
     * @param assertNoChange are we doing a simple comparison for change detection?
     * @param destinationFilePath destination file location.
     * @param javaClass fully qualified class name.
     * @throws IOException
     */
    static void finalizeGeneration(final StringBuilder code, final boolean assertNoChange,
            final String destinationFilePath, final String javaClass, final String gradleTask) throws IOException {
        if (assertNoChange) {
            String oldCode = new String(Files.readAllBytes(Paths.get(destinationFilePath)));

            if (!code.toString().equals(oldCode)) {
                throw new RuntimeException("Change in generated code for class:" + javaClass +
                        " with Python file at " + destinationFilePath + ".\n " +
                        "Run the code generation task (" + gradleTask + ") to regenerate, " +
                        "followed by \"git diff " + destinationFilePath + "\" to see the changes." +
                        "\n\n" +
                        "To diagnose possible indeterminism in the generation process, regenerate " +
                        "the code and check the diff **multiple times**.");
            }

        } else {
            final File pythonFile = new File(destinationFilePath);

            if (!pythonFile.exists()) {
                throw new RuntimeException("File: " + destinationFilePath + " does not exist.");
            }

            try (final PrintWriter out = new PrintWriter(pythonFile)) {
                out.print(code);
            }
        }
    }

    /**
     * Get the default list for documentation root, ordered by precedence.
     * 
     * @param devroot the development root.
     * @return the doc root list.
     */
    static ArrayList<String> getDefaultDocRoot(final String devroot) {
        ArrayList<String> docRoot = new ArrayList<>();
        docRoot.add(devroot + "/Integrations/python/deephaven/docCustom");
        docRoot.add(devroot + "/Integrations/python/deephaven/doc");
        return docRoot;
    }

    /**
     * Get the List of DocStringContainers corresponding to the java class specified by path and the documentation roots
     * specified in docRoot.
     *
     * Note that it is currently expected that many classes have no docs populated, so nulls (corresponding to missing
     * docs) are silently skipped.
     * 
     * @param path fully qualified path name for the java class.
     * @param docRoot the doc root list.
     * @return the DocstringContainer list, in order of preference.
     */
    static List<DocstringContainer> getDocstringContainer(final String path, final List<String> docRoot,
            final Logger log) {
        ArrayList<DocstringContainer> docs = new ArrayList<>();
        for (String root : docRoot) {
            DocstringContainer tempClass = loadJsonDoc(path, root, log);
            if (tempClass != null) {
                docs.add(tempClass);
            }
        }
        return docs;
    }

    /**
     * Loads the DocstringContainer for the class with path javaClass, relative to the rootDirectory.
     * 
     * @param javaClass fully qualified path name for the java class.
     * @param rootDirectory root directory below the json files are located.
     * @return DocstringContainer for javaClass, which will be null if there is no corresponding json file.
     */
    static DocstringContainer loadJsonDoc(final String javaClass, final String rootDirectory, final Logger log) {
        // get reference to appropriate json file location
        String classPath = javaClass.replace('.', '/');
        classPath = classPath.replace('$', '/');
        String pathName = rootDirectory + File.separator + classPath + ".json";
        File jsonFile = new File(pathName);

        if (jsonFile.exists()) {
            // parse using Jackson ObjectMapper
            ObjectMapper objectMapper = new ObjectMapper();
            DocstringContainer out;
            try {
                out = objectMapper.readValue(jsonFile, DocstringContainer.class);
            } catch (IOException e) {
                log.warning("Parsing of file " + jsonFile + " as a DocstringContainer class instance failed, " +
                        "and is being skipped!");
                out = null;
            }
            return out;
        } else {
            log.info("No documentation file found for class " + javaClass + " at " + jsonFile);
            return null;
        }
    }

    /**
     * Format the input string as a (multiline) Python docstring. It is assumed that the beginning/trailing quotation
     * marks are not present, and there is no indentation formatting.
     * 
     * @param input the input string.
     * @param indent the number of spaces by which to indent.
     * @return the formatted string.
     */
    static String formatDocstring(String input, int indent) {
        if (StringUtils.isNullOrEmpty(input)) {
            return "";
        }

        if (!input.startsWith("\n")) {
            input = "\n" + input;
        }
        if (!input.endsWith("\n")) {
            input += "\n";
        }

        input = "\n\"\"\"" + input + "\"\"\"\n";

        String replace = "\n";
        for (int i = 0; i < indent; i++) {
            replace += " ";
        }
        return input.replaceAll("\n", replace);
    }

    /**
     * Fetch the doc string for a class from a List of DocstringContainers and format appropriately. The priority of the
     * DocstringContainers is implicit in their order in the List, so the first one (in List order) which returns a
     * non-null docstring will be used.
     * 
     * @param docList List of the DocstringContainer object for the class in question
     * @param indent the number of spaces by which to indent
     * @return the formatted string (or empty String, if no docstring for `method` anywhere in `docList`)
     */
    static String getClassDocstring(final List<DocstringContainer> docList, final int indent) {
        if (docList == null) {
            return "";
        }

        String docstring = null;
        for (DocstringContainer docs : docList) {
            if (docs == null) {
                continue;
            }

            docstring = docs.getText();
            if (docstring != null) {
                break;
            }
        }
        return formatDocstring(docstring, indent);
    }

    /**
     * Fetch the doc string for a given method from a List of DocstringContainers and format appropriately. The priority
     * of the DocstringContainers is implicit in their order in the List, so the first one (in List order) which returns
     * a non-null docstring will be used.
     * 
     * @param docList List of the DocstringContainer object for the class in question
     * @param method the name of the method
     * @param indent the number of spaces by which to indent
     * @return the formatted string (or empty String, if no docstring for `method` anywhere in `docList`)
     */
    static String getMethodDocstring(final List<DocstringContainer> docList, final String method, final int indent) {
        if (docList == null) {
            return "";
        }

        String docstring = null;
        for (DocstringContainer docs : docList) {
            if (docs == null) {
                continue;
            }

            docstring = docs.getMethods().get(method);
            if (docstring != null) {
                break;
            }
        }
        return formatDocstring(docstring, indent);
    }

    /**
     * Container for all signatures of a given method
     */
    public static class MethodSignatureCollection {
        private final String methodName;
        private final List<MethodSignature> signatures = new ArrayList<>();

        public MethodSignatureCollection(final String methodName) {
            this.methodName = methodName;
        }

        public void addFunction(GroovyStaticImportGenerator.JavaFunction function) {
            signatures.add(new MethodSignature(function));
        }

        public String getMethodName() {
            return methodName;
        }

        public List<MethodSignature> getSignatures() {
            return signatures;
        }

        /**
         * Find the commonalities among all the overloads - this is specifically for Python wrapping of this function.
         * 
         * @return the common version of all overloads.
         */
        public MethodSignature reduceSignature() {
            final HashSet<String> paramsSet = new HashSet<>();
            final HashSet<Boolean> varArgsSet = new HashSet<>();
            final HashSet<Type> typeSet = new HashSet<>();
            final HashSet<Class> classSet = new HashSet<>();
            for (final MethodSignature sig : signatures) {
                paramsSet.add(sig.getParams());
                varArgsSet.add(sig.getIsVarArgs());
                typeSet.add(sig.getReturnType());
                classSet.add(sig.getReturnClass());
            }
            final String thisParams;
            final Boolean thisVarArgs;
            Type thisType = null;
            Class thisClass = null;
            if (typeSet.size() == 1) {
                thisType = signatures.get(0).getReturnType();
            }

            if (classSet.size() == 1) {
                thisClass = signatures.get(0).getReturnClass();
            }
            if (paramsSet.size() == 1) { // all function signatures use the same naming scheme
                thisParams = signatures.get(0).getParams();
                if (varArgsSet.size() == 1) {
                    thisVarArgs = signatures.get(0).getIsVarArgs();
                } else {
                    thisVarArgs = true;
                }
            } else {
                thisParams = "args";
                thisVarArgs = true;
            }
            return new MethodSignature(thisParams, thisVarArgs, thisType, thisClass);
        }
    }

    /**
     * Container for function signature details for one given function.
     */
    public static class MethodSignature {
        private final String params;
        private final Boolean isVarArgs;
        private final Type returnType;
        private final Class returnClass;

        MethodSignature(final String params, final Boolean isVarArgs, final Type returnType, final Class returnClass) {
            this.params = params;
            this.isVarArgs = isVarArgs;
            this.returnType = returnType;
            this.returnClass = returnClass;
        }

        MethodSignature(GroovyStaticImportGenerator.JavaFunction function) {
            this.params = String.join(",", function.getParameterNames());
            this.isVarArgs = function.isVarArgs();
            this.returnType = function.getReturnType();
            this.returnClass = function.getReturnClass();
        }

        public String getParams() {
            return params;
        }

        public Boolean getIsVarArgs() {
            return isVarArgs;
        }

        public Type getReturnType() {
            return returnType;
        }

        public Class getReturnClass() {
            return returnClass;
        }

        /**
         * Create the appropriate Python parameters string for this function signature.
         * 
         * @param joinString string on which to join the arguments.
         * @param addSelf option to prepend with self for the methods wrapping to class methods.
         * @return parameter string.
         */
        public String createPythonParams(final String joinString, final boolean addSelf) {
            final String[] names = params.split(",");
            String out = "";
            if (names.length > 0) {
                for (int i = 0; i < names.length - 1; i++) {
                    out += names[i] + joinString;
                }
                if (isVarArgs) {
                    out += "*";
                }
                out += names[names.length - 1];
            }
            if (addSelf) {
                out = (out.length() < 1 ? "self" : "self" + joinString + out);
            }
            return out;
        }

        public String createPythonParams() {
            return createPythonParams(", ", false);
        }

        public String createPythonParams(final boolean addSelf) {
            return createPythonParams(", ", addSelf);
        }

    }

    /**
     * Container for all method signature details.
     */
    public static class MethodContainer {
        private final Map<String, MethodSignatureCollection> methodSignatures = new LinkedHashMap<>();

        MethodContainer(GroovyStaticImportGenerator.JavaFunction[] sortedMethods) {
            for (final GroovyStaticImportGenerator.JavaFunction function : sortedMethods) {
                final String methodName = function.getMethodName();
                methodSignatures.computeIfAbsent(methodName, MethodSignatureCollection::new).addFunction(function);
            }
        }

        public Map<String, MethodSignatureCollection> getMethodSignatures() {
            return methodSignatures;
        }
    }

    /**
     * Container for parsed argument details.
     */
    public static class GeneratorElement {
        private final String classString;
        private final String preambleFile;
        private final String destinationFile;

        GeneratorElement(String classString, String preambleFile, String destinationFile) {
            this.classString = classString;
            this.preambleFile = preambleFile;
            this.destinationFile = destinationFile;
        }

        public String getClassString() {
            return classString;
        }

        public String getPreambleFile() {
            return preambleFile;
        }

        public String getDestinationFile() {
            return destinationFile;
        }
    }

    /**
     * Container for docstring details, which should have been extracted from the Java docs and populated into json
     * files by a separate task.
     *
     * Instances of this class are expected to be constructed auto-magically by json parsing using the Jackson library.
     */
    public static class DocstringContainer {
        private String className;
        private Map<String, String> methods;
        private String path;
        private String text;
        private String typeName;

        public void setClassName(String className) {
            this.className = className;
        }

        public String getClassName() {
            return className;
        }

        public void setMethods(Map<String, String> methods) {
            this.methods = methods;
        }

        public Map<String, String> getMethods() {
            return methods;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getPath() {
            return path;
        }

        public void setText(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }
    }
}
