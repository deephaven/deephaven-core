import org.gradle.api.Project
import org.gradle.api.file.CopySpec
import org.gradle.api.tasks.Sync

enum PySpec {
  WORKER_STANDARD_PY36(PythonVersion.PY_36, PySpecType.WORKER_STANDARD, PythonConstants.INCLUDE_IN_RPM),
  WORKER_STANDARD_PY37(PythonVersion.PY_37, PySpecType.WORKER_STANDARD, PythonConstants.INCLUDE_IN_RPM),

  WORKER_JUPYTER_PY36(PythonVersion.PY_36, PySpecType.WORKER_JUPYTER, PythonConstants.INCLUDE_IN_RPM),
  WORKER_JUPYTER_PY37(PythonVersion.PY_37, PySpecType.WORKER_JUPYTER, PythonConstants.INCLUDE_IN_RPM),

  JPY_ONLY_PY36(PythonVersion.PY_36, PySpecType.JPY_ONLY, PythonConstants.EXCLUDE_FROM_RPM),
  JPY_ONLY_PY37(PythonVersion.PY_37, PySpecType.JPY_ONLY, PythonConstants.EXCLUDE_FROM_RPM),

  JUPYTER_NOTEBOOK_SERVER_PY36(PythonVersion.PY_36, PySpecType.JUPYTER_NOTEBOOK_SERVER, PythonConstants.INCLUDE_IN_RPM),
  JUPYTER_NOTEBOOK_SERVER_PY37(PythonVersion.PY_37, PySpecType.JUPYTER_NOTEBOOK_SERVER, PythonConstants.INCLUDE_IN_RPM);

  final PythonVersion version
  final PySpecType type
  final boolean isInRpm

  PySpec(PythonVersion version, PySpecType type, boolean isInRpm) {
    this.version = version
    this.type = type
    this.isInRpm = isInRpm
  }

  File getGeneratedRequirements(Project project) {
    type.getGeneratedRequirementsPath(version.getProject(project))
  }

  String getRpmPath() {
    "${version.rpmPath}/specs/${type.type}"
  }

  Sync createGenerateRequirementsTask(Project project) {
    return project.tasks.create("generate-requirements-${type.type}", Sync, { Sync sync ->
      sync.group = "python"
      sync.description = "Generate the requirements.txt for spec '${type.type}'"

      sync.from "specs/${type.type}/requirements.txt"
      sync.into new File(project.buildDir, "specs/${type.type}")

      def projectVersion = project.version
      def buildCommand = "./gradlew ${project.name}:${sync.name}"
      def buildTime = BuildConstants.BUILD_TIME.toString()
      def tokens = [
          project: [
              version: projectVersion
          ],
          build: [
              command: buildCommand,
              time: buildTime
          ]
      ]

      sync.expand tokens
      sync.inputs.property("project.version", projectVersion)
      sync.inputs.property("build.command", buildCommand)
      // note: build.time is *not* an important input
      return
    })
  }
}
