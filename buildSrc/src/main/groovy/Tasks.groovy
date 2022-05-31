import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginConvention
import org.gradle.api.tasks.JavaExec
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.TaskProvider

/**
 * Task registration helpers
 */
@CompileStatic
class Tasks {

    static TaskProvider<? extends JavaExec> registerMainExecTask(Project project, String taskName, String mainClassName) {
        return project.tasks.register(taskName, JavaExec) { task ->
            JavaPluginConvention java = project.convention.plugins.get('java') as JavaPluginConvention
            SourceSet sourceSet = java.sourceSets.getByName('main')
            task.workingDir project.rootDir
            task.classpath = sourceSet.runtimeClasspath
            task.main = mainClassName
            task.systemProperty 'Configuration.rootFile', 'dh-defaults.prop'
        }
    }

    static TaskProvider<? extends JavaExec> registerTestExecTask(Project project, String taskName, String mainClassName) {
        return project.tasks.register(taskName, JavaExec) { task ->
            JavaPluginConvention java = project.convention.plugins.get('java') as JavaPluginConvention
            SourceSet sourceSet = java.sourceSets.getByName('test')
            task.workingDir project.rootDir
            task.classpath = sourceSet.runtimeClasspath
            task.main = mainClassName
            task.systemProperty 'Configuration.rootFile', 'dh-tests.prop'
        }
    }
}
