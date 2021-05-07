import groovy.transform.CompileStatic

import java.time.Instant

@CompileStatic
final class BuildConstants {
    static final Instant BUILD_TIME = Instant.now()
}
