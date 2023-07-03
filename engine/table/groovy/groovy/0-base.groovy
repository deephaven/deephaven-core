__QUERY_NAME__="core"

source = {fileName ->
    System.err.println("source(fileName) is deprecated for removal, please consider import, or provide your own implementation")
    __groovySession.runScript(fileName)
}

sourceOnce = {fileName ->
    System.err.println("sourceOnce(fileName) is deprecated for removal, please consider import, or provide your own implementation")
    __groovySession.runScriptOnce(fileName)
}

scriptImportClass = {c -> __groovySession.addScriptImportClass(c)}
scriptImportStatic = {c -> __groovySession.addScriptImportStatic(c)}

isValidVariableName = {name -> name.matches("^[a-zA-Z_][a-zA-Z_0-9]*")}

publishVariable = { String name, value ->
    if(!isValidVariableName(name)){
        throw new RuntimeException("publishVariable: Attempting to publish an invalid variable name: " + name)
    }

    binding.setVariable(name, value)
}

removeVariable = {name ->
    if(!isValidVariableName(name)){
        throw new RuntimeException("removeVariable: Attempting to remove an invalid variable name: " + name)
    }

    binding.variables.remove(name)
}
