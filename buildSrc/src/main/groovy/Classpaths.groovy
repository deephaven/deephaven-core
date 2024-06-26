import groovy.transform.CompileStatic
import org.gradle.api.Action
import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.artifacts.Dependency
import org.gradle.api.internal.artifacts.dependencies.DefaultExternalModuleDependency
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

    static final String GWT_GROUP = 'org.gwtproject'
    static final String GWT_VERSION = '2.11.0'

    // TODO (core#1163): take advantage of symbol-solver-core
//    static final String JAVA_PARSER_NAME = 'javaparser-symbol-solver-core'

    static final String JETTY_VERSION = '9.4.44.v20210927'

    // TODO(deephaven-core#1685): Create strategy around updating and maintaining protoc version

    static final String HADOOP_GROUP = 'org.apache.hadoop'
    static final String HADOOP_VERSION = '3.4.0'

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

    static void inheritGwt(Project p, String name, String configName) {
        Configuration config = p.configurations.getByName(configName)
        if (addDependency(config, GWT_GROUP, name, GWT_VERSION)) {
            // when we add gwt-dev, lets also force asm version, just to be safe.
            name == 'gwt-dev' && config.resolutionStrategy {
                force 'org.ow2.asm:asm:9.2'
                force 'org.ow2.asm:asm-util:9.2'
                force 'org.ow2.asm:asm-commons:9.2'
            }
        }
    }

    /** configName controls only the Configuration's classpath, all transitive dependencies are runtimeOnly */
    static void inheritParquetHadoopConfiguration(Project p, String configName = JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME) {
        Configuration config = p.configurations.getByName(configName)
        addDependency(config, HADOOP_GROUP, 'hadoop-common', HADOOP_VERSION) {
            it.setTransitive(false)
            // Do not take any extra dependencies of this project transitively. We just want a few classes for
            // configuration and compression codecs. For any additional required dependencies, add them separately, as
            // done for woodstox, shaded-guava, etc. below. Or we can replace setTransitive(false) here with more
            // exclusions (we want to avoid pulling in netty, loggers, jetty-util, guice and asm).
        }

        Configuration runtimeOnly = p.configurations.getByName(JavaPlugin.RUNTIME_ONLY_CONFIGURATION_NAME)
        addDependency(runtimeOnly, 'com.fasterxml.woodstox', 'woodstox-core', '6.6.2') {
            it.because('hadoop-common required dependency for Configuration')
        }
        addDependency(runtimeOnly, 'org.apache.hadoop.thirdparty', 'hadoop-shaded-guava', '1.2.0') {
            it.because('hadoop-common required dependency for Configuration')
        }
        addDependency(runtimeOnly, 'commons-collections', 'commons-collections', '3.2.2') {
            it.because('hadoop-common required dependency for Configuration')
        }
    }

    static void inheritIcebergHadoop(Project p, String configName = JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME) {
        Configuration config = p.configurations.getByName(configName)
        inheritParquetHadoopConfiguration(p, configName)
        addDependency(config, HADOOP_GROUP, 'hadoop-hdfs-client', HADOOP_VERSION)
    }
}
