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

    static final String JETTY_VERSION = '9.4.44.v20210927'

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
}
