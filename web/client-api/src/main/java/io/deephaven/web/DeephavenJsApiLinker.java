package io.deephaven.web;

import com.google.gwt.core.ext.LinkerContext;
import com.google.gwt.core.ext.TreeLogger;
import com.google.gwt.core.ext.UnableToCompleteException;
import com.google.gwt.core.ext.linker.*;
import com.google.gwt.dev.About;
import com.google.gwt.dev.util.DefaultTextOutput;

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
        out.print("(function(){");
        out.newline();

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
        out.print("self.dh = {}");
        out.newline();
        out.print("var $wnd = self, $doc, $entry, $moduleName, $moduleBase;");
        out.newline();
        out.print("var $gwt_version = \"" + About.getGwtVersionNum() + "\";");
        out.newlineOpt();
        out.print(javaScript[0]);
        out.newline();

        out.print("gwtOnLoad(null,'" + context.getModuleName() + "',null);");
        out.newline();
        out.print("})();");
        out.newline();

        toReturn.add(emitString(logger, out.toString(), "dh-core.js"));

        return toReturn;
    }

}
