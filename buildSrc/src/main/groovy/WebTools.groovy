import org.gradle.api.Task

/**
 * Tools used by both GWT and Node web builds.
 */
class WebTools {

    static boolean shouldRun(Task t) {
        if (t.project.findProperty('skipWeb') == 'true') {
            t.logger.quiet 'skipWeb flag detected, excluding web build task {}', t.path
            return false
        }
        return true
    }

}
