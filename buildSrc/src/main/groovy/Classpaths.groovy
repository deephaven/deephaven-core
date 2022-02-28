import groovy.transform.CompileStatic
import org.gradle.api.Action
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.internal.artifacts.dependencies.DefaultExternalModuleDependency
import org.gradle.api.plugins.JavaPlatformPlugin
import org.gradle.api.plugins.JavaPlugin
import org.gradle.internal.Actions

/**
 * A centralized utility for adding classpaths to projects.
 *
 * This is the most efficient way to standardize dependencies,
 * as it does not add any unnecessary configurations "just in case we need them"
 * (i.e. what was done in the original gradle refactor).
 *
 * To use:
 * Classpaths.inheritSomething(project)
 */
@CompileStatic
class Classpaths {

    static final String ELEMENTAL_GROUP = 'com.google.elemental2'
    static final String ELEMENTAL_VERSION = '1.0.0-RC1'

    static final String GWT_GROUP = 'com.google.gwt'
    static final String GWT_VERSION = '2.8.2'

    static final String JAVA_PARSER_GROUP = 'com.github.javaparser'
    static final String JAVA_PARSER_NAME = 'javaparser-core'
    static final String JAVA_PARSER_VERSION = '3.23.0'
    // TODO (core#1163): take advantage of symbol-solver-core
//    static final String JAVA_PARSER_NAME = 'javaparser-symbol-solver-core'

    static final String JAVAX_ANNOTATIONS_GROUP = 'javax.validation'
    static final String JAVAX_ANNOTATIONS_NAME = 'validation-api'
    static final String JAVAX_ANNOTATIONS_VERSION = '1.0.0.GA'

    static final String JETTY_GROUP = 'org.eclipse.jetty'
    static final String JETTY_VERSION = '9.4.20.v20190813'

    static final String JGIT_VERSION = '5.8.1.202007141445-r'

    static final String JS_INTEROP_GROUP = 'com.google.jsinterop'
    static final String JS_INTEROP_VERSION = '1.0.2'

    static final String COMMONS_GROUP = 'org.apache.commons'

    static final String ARROW_GROUP = 'org.apache.arrow'
    static final String ARROW_VERSION = '6.0.1'

    static final String SLF4J_GROUP = 'org.slf4j'
    static final String SLF4J_VERSION = '2.0.0-alpha5'

    static final String FLATBUFFER_GROUP = 'com.google.flatbuffers'
    static final String FLATBUFFER_NAME = 'flatbuffers-java'
    static final String FLATBUFFER_VERSION = '1.12.0'

    static final String DAGGER_GROUP = 'com.google.dagger'
    static final String DAGGER_NAME = 'dagger'
    static final String DAGGER_COMPILER = 'dagger-compiler'
    static final String DAGGER_VERSION = '2.31.1'

    static final String AUTOSERVICE_GROUP = 'com.google.auto.service'
    static final String AUTOSERVICE_NAME = 'auto-service-annotations'
    static final String AUTOSERVICE_COMPILER = 'auto-service'
    static final String AUTOSERVICE_VERSION = '1.0.1'

    static final String IMMUTABLES_GROUP = 'org.immutables'
    static final String IMMUTABLES_NAME = 'value'
    static final String IMMUTABLES_VERSION = '2.8.1'

    static final String JUNIT_GROUP = 'org.junit'
    static final String JUNIT_NAME = 'junit-bom'
    static final String JUNIT_VERSION = '5.7.2'

    static final String ASSERTJ_GROUP = 'org.assertj'
    static final String ASSERTJ_NAME = 'assertj-core'
    static final String ASSERTJ_VERSION = '3.19.0'

    static final String LOGBACK_GROUP = 'ch.qos.logback'
    static final String LOGBACK_NAME = 'logback-classic'
    static final String LOGBACK_VERSION = '1.3.0-alpha12'

    static final String GROOVY_GROUP = 'org.codehaus.groovy'
    static final String GROOVY_VERSION = '3.0.9'

    static final String GRPC_GROUP = 'io.grpc'
    static final String GRPC_VERSION = '1.42.1'

    static boolean addDependency(Configuration conf, String group, String name, String version, Action<? super DefaultExternalModuleDependency> configure = Actions.doNothing()) {
        if (!conf.dependencies.find { it.name == name && it.group == group}) {
            DefaultExternalModuleDependency dep = dependency group, name, version
            configure.execute(dep)
            conf.dependencies.add(dep)
            true
        }
        false
    }

    static void addDependency(Configuration conf, Dependency dep) {
        conf.dependencies.add(dep)
    }

    static DefaultExternalModuleDependency dependency(String group, String name, String version) {
        new DefaultExternalModuleDependency(group, name, version)
    }

    static Configuration compile(Project p) {
        p.configurations.findByName('api') ?: p.configurations.getByName('compile')
    }

    static void inheritGwt(Project p, String name = 'gwt-user', String configName = 'compileOnly') {
        Configuration config = p.configurations.getByName(configName)
        if (addDependency(config, GWT_GROUP, name, GWT_VERSION)) {
            // when we add gwt-dev, lets also force asm version, just to be safe.
            name == 'gwt-dev' && config.resolutionStrategy {
                force 'org.ow2.asm:asm:5.0.3'
                force 'org.ow2.asm:asm-util:5.0.3'
                force 'org.ow2.asm:asm-commons:5.0.3'
            }
        }
    }

    static void inheritJavaParser(Project p, String name = JAVA_PARSER_NAME) {
        Configuration compile = compile p
        addDependency compile, JAVA_PARSER_GROUP, name, JAVA_PARSER_VERSION
    }

    static void inheritJavaxAnnotations(Project p) {
        Configuration compile = compile p
        addDependency compile, JAVAX_ANNOTATIONS_GROUP, JAVAX_ANNOTATIONS_NAME, JAVAX_ANNOTATIONS_VERSION
    }

    static void inheritJsInterop(Project p, String name = 'base') {
        Configuration compile = compile p
        addDependency compile, JS_INTEROP_GROUP, name,
                // google is annoying, and have different versions released for the same groupId
                // :base: is the only one that is different, so we'll use it in the ternary.
                name == 'base'? '1.0.0-RC1' : JS_INTEROP_VERSION
    }

    static void inheritElemental(Project p, String name = 'elemental2-core') {
        Configuration compile = compile p
        addDependency compile, ELEMENTAL_GROUP, name, ELEMENTAL_VERSION
    }

    static void inheritCommonsText(Project p) {
        Configuration compile = compile p
        addDependency compile, COMMONS_GROUP, 'commons-text', "1.6", {
            // commons-text depends on commons-lang3; sadly, our version of lang3 is so old,
            // there is no version of commons-text which depends on it.  So, we just exclude it.
            // we only want some small, self-contained classes in commons-text anyway.
            dep -> dep.exclude(['module': 'commons-lang3'])
        }
    }

    static void inheritArrow(Project p, String name, String configName) {
        Configuration config = p.configurations.getByName(configName)
        addDependency(config, ARROW_GROUP, name, ARROW_VERSION)
    }

    static void inheritFlatbuffer(Project p, String configName) {
        Configuration config = p.configurations.getByName(configName)
        addDependency(config, FLATBUFFER_GROUP, FLATBUFFER_NAME, FLATBUFFER_VERSION)
    }

    static void inheritDagger(Project p, boolean test = false) {
        Configuration ic = p.configurations.getByName(test ? 'testImplementation' : 'implementation')
        addDependency(ic, DAGGER_GROUP, DAGGER_NAME, DAGGER_VERSION)
        Configuration ap = p.configurations.getByName(test ? 'testAnnotationProcessor' : 'annotationProcessor')
        addDependency(ap, DAGGER_GROUP, DAGGER_COMPILER, DAGGER_VERSION)
    }

    /**
     * Auto service is an annotation processor that will generate META-INF/services/ files.
     *
     * @see <a href="https://github.com/google/auto/tree/master/service">google/auto/tree/master/service</a>
     */
    static void inheritAutoService(Project p, boolean test = false) {
        Configuration ic = p.configurations.getByName(test ? 'testCompileOnly' : 'compileOnly')
        addDependency(ic, AUTOSERVICE_GROUP, AUTOSERVICE_NAME, AUTOSERVICE_VERSION)
        Configuration ap = p.configurations.getByName(test ? 'testAnnotationProcessor' : 'annotationProcessor')
        addDependency(ap, AUTOSERVICE_GROUP, AUTOSERVICE_COMPILER, AUTOSERVICE_VERSION)
    }

    static void inheritImmutables(Project p) {
        Configuration ap = p.configurations.getByName('annotationProcessor')
        addDependency(ap, IMMUTABLES_GROUP, IMMUTABLES_NAME, IMMUTABLES_VERSION)
        p.getDependencies().add('compileOnly', p.project(':util-immutables'))
    }

    static void inheritJUnitPlatform(Project p, String configName = JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME) {
        Configuration config = p.configurations.getByName(configName)
        addDependency(config, p.getDependencies().platform(JUNIT_GROUP + ":" + JUNIT_NAME + ":" + JUNIT_VERSION))
    }

    static void inheritAssertJ(Project p, String configName = JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME) {
        Configuration config = p.configurations.getByName(configName)
        addDependency(config, ASSERTJ_GROUP, ASSERTJ_NAME, ASSERTJ_VERSION)
    }

    static void inheritLogbackClassic(Project p, String configName = JavaPlugin.RUNTIME_ONLY_CONFIGURATION_NAME) {
        Configuration config = p.configurations.getByName(configName)
        addDependency(config, LOGBACK_GROUP, LOGBACK_NAME, LOGBACK_VERSION)
    }

    static void inheritSlf4j(Project p, String name, String configName) {
        Configuration config = p.configurations.getByName(configName)
        addDependency(config, SLF4J_GROUP, name, SLF4J_VERSION)
    }

    static void inheritGroovy(Project p, String name, String configName) {
        Configuration config = p.configurations.getByName(configName)
        addDependency(config, GROOVY_GROUP, name, GROOVY_VERSION)
    }

    static void inheritGrpcPlatform(Project p, String configName = JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME) {
        Configuration config = p.configurations.getByName(configName)
        addDependency(config, p.getDependencies().platform(GRPC_GROUP + ":grpc-bom:" + GRPC_VERSION))
    }
}
