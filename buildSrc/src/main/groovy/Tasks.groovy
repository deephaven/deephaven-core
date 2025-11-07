import groovy.transform.CompileStatic
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension
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
            SourceSet sourceSet = project.extensions.findByType(JavaPluginExtension).sourceSets.getByName('main')
            task.workingDir project.rootDir
            task.classpath = sourceSet.runtimeClasspath
            task.mainClass.set mainClassName
            task.systemProperty 'Configuration.rootFile', 'dh-defaults.prop'
        }
    }

    static TaskProvider<? extends JavaExec> registerTestExecTask(Project project, String taskName, String mainClassName) {
        return project.tasks.register(taskName, JavaExec) { task ->
            SourceSet sourceSet = project.extensions.findByType(JavaPluginExtension).sourceSets.getByName('main')
            task.workingDir project.rootDir
            task.classpath = sourceSet.runtimeClasspath
            task.mainClass.set mainClassName
            task.systemProperty 'Configuration.rootFile', 'dh-tests.prop'
        }
    }
}
