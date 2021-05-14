This directory contains "project roots" for our "bin" projects.

The :configs and :test-configs projects all put
their resources directly into the project root directory.

They would normally go into src/main/resources for a java project,
so this non-standard structure confuses IntelliJ, and prevents the
use of the Multiple Modules Per SourceSet feature.

So, we create empty directories here to serve as project root,
and then hook up the "real" location as the SourceSet resources directory.

This gives IntelliJ multiple directories (which do not intersect) to mark as Modules.

Do not replicate this structure for future modules.
Instead, prefer standard java module structure for new modules:
src/main/java and src/main/resources (maven conventions, used by open api)
or
java and resources (legacy deephaven conventions)
