package io.deephaven.util.text;

public class ScriptSanitizer {
    /**
     * Replaces unwanted characters like smart quotes with the standard equivalent. Used so that copy and paste
     * operations from tools like Outlook or Slack don't result in unusable scripts.
     *
     * @param commandToSanitize the command to sanitize
     * @return the command with unwanted characters replaced with allowable characters
     */
    public static String sanitizeScriptCode(String commandToSanitize) {
        // smart quotes
        commandToSanitize = commandToSanitize.replace("\u201C", "\"");
        commandToSanitize = commandToSanitize.replace("\u201D", "\"");
        return commandToSanitize;
    }
}
