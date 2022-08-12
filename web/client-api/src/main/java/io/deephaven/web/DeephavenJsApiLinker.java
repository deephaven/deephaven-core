/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web;

import com.google.gwt.core.ext.LinkerContext;
import com.google.gwt.core.ext.TreeLogger;
import com.google.gwt.core.ext.UnableToCompleteException;
import com.google.gwt.core.ext.linker.*;
import com.google.gwt.dev.About;
import com.google.gwt.dev.util.DefaultTextOutput;
import com.google.gwt.util.tools.Utility;

import java.io.IOException;
import java.util.Set;

/**
 * Basic Linker for the Deephaven JS API.
 */
@Shardable
@LinkerOrder(LinkerOrder.Order.PRIMARY)
public class DeephavenJsApiLinker extends AbstractLinker {

    @Override
    public String getDescription() {
        return "DeephavenJsApiLinker - Generate the JS for the Deephaven JS API";
    }

    @Override
    public ArtifactSet link(TreeLogger logger, LinkerContext context, ArtifactSet artifacts, boolean onePermutation)
            throws UnableToCompleteException {
        return this.link(logger, context, artifacts);
    }

    public ArtifactSet link(TreeLogger logger, LinkerContext context, ArtifactSet artifacts)
            throws UnableToCompleteException {

        ArtifactSet toReturn = new ArtifactSet(artifacts);
        DefaultTextOutput out = new DefaultTextOutput(true);

        // get compilation result
        Set<CompilationResult> results = artifacts.find(CompilationResult.class);
        if (results.size() == 0) {
            logger.log(TreeLogger.WARN, "Requested 0 permutations");
            return toReturn;
        }

        if (results.size() > 1) {
            logger.log(TreeLogger.ERROR, "Expected 1 permutation, found " + results.size() + " permutations.");
            throw new UnableToCompleteException();
        }

        CompilationResult result = results.iterator().next();

        // get the generated javascript
        String[] javaScript = result.getJavaScript();

        StringBuffer buffer = readFileToStringBuffer(getSelectionScriptTemplate(), logger);
        replaceAll(buffer, "__GWT_VERSION__",  About.getGwtVersionNum());
        replaceAll(buffer, "__JAVASCRIPT_RESULT__",  javaScript[0]);
        replaceAll(buffer, "__MODULE_NAME__",  context.getModuleName());

        out.print(buffer.toString());

        toReturn.add(emitString(logger, out.toString(), "dh-core.js"));

        return toReturn;
    }
    protected StringBuffer readFileToStringBuffer(String filename,
                                                  TreeLogger logger) throws UnableToCompleteException {
        StringBuffer buffer;
        try {
            buffer = new StringBuffer(Utility.getFileFromClassPath(filename));
        } catch (IOException e) {
            logger.log(TreeLogger.ERROR, "Unable to read file: " + filename, e);
            throw new UnableToCompleteException();
        }
        return buffer;
    }
    protected String getSelectionScriptTemplate(){
        return "io/deephaven/web/DeephavenJsApiLinkerTemplate.js";
    }
    protected static void replaceAll(StringBuffer buf, String search,
                                     String replace) {
        int len = search.length();
        for (int pos = buf.indexOf(search); pos >= 0; pos = buf.indexOf(search,
                pos + 1)) {
            buf.replace(pos, pos + len, replace);
        }
    }
}
