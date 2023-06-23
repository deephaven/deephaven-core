import io.github.classgraph.ClassGraph
import io.deephaven.extensions.ClassGraphExtension

def help() {
    println ""
    println "================================================="
    println "Hello, from classgraph-init.groovy!"
    println ""
    println "The tables `groovyTable`, `classTable`, `txtTable`, and `jsonTable` have been created as examples."
    println ""
    println "You can call help() to see this message again, or exit() to exit."
    println "================================================="
    println ""
}

def exit() {
    System.exit(0)
}

help()

help = { -> help() }
exit = { -> exit() }

def scan = new ClassGraph().scan()
try {
    groovyTable = ClassGraphExtension.tableAllExtensions("groovy", scan)
    classTable = ClassGraphExtension.tableAllExtensions("class", scan)
    txtTable = ClassGraphExtension.tableAllExtensions("txt", scan)
    jsonTable = ClassGraphExtension.tableAllExtensions("json", scan)
} finally {
    scan.close()
}
