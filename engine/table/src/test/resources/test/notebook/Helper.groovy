package test.notebook

return "Helper"

static String getVersion() {
    return "SERVER"
}

static int getValue() {
    return 100
}

class HelperClass {
    final String source = "SERVER"
    String getSource() {
        return source
    }
}

static String getSourceViaClass() {
    new HelperClass().getSource()
}
