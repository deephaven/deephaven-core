import org.gradle.api.Project

enum PySpecType {
  WORKER_STANDARD("worker-standard"),
  WORKER_JUPYTER("worker-jupyter"),
  JUPYTER_NOTEBOOK_SERVER("jupyter-notebook-server"),
  JPY_ONLY("jpy-only");

  final String type

  PySpecType(String type) {
    this.type = type
  }

  File getGeneratedRequirementsPath(Project specificProject) {
    new File(specificProject.buildDir, "specs/${type}/requirements.txt")
  }
}