package test.notebook

return "MyDependency"

static String getVersion() {
    return "SERVER"
}

static int getValue() {
    return 100
}

class Inner {
    final String source = "SERVER"
    String getSource() {
        return source
    }
}

static String getSourceViaClass() {
    new Inner().getSource()
}
