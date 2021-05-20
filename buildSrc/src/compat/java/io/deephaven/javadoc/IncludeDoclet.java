package io.deephaven.javadoc;

import com.sun.source.doctree.BlockTagTree;
import com.sun.source.doctree.DocCommentTree;
import com.sun.source.doctree.DocTree;
import com.sun.source.util.DocTrees;
import jdk.javadoc.doclet.DocletEnvironment;
import jdk.javadoc.doclet.Reporter;
import jdk.javadoc.doclet.StandardDoclet;
import jdk.javadoc.internal.tool.DocEnvImpl;

import javax.lang.model.SourceVersion;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;
import javax.tools.Diagnostic;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import java.util.*;

/**
 * This doclet uses the custom tags "IncludeAll", "Include" and "Exclude"
 * to (in part) determine whether javadoc is generated.
 *
 * IncludeAll- add at the beginning of a Class to include all methods, fields, etc. by default. Note inner classes must
 * have their own IncludeAll/Include tags to be included. If an IncludeAll is added to a non-Class, it behaves as an Include.
 * Include- add to a class, method, or field to include inside the produced javadoc.
 * Exclude- add to a class, method, or field to exclude from the produced javadoc. Only necessary after an IncludeAll tag
 */
public class IncludeDoclet extends StandardDoclet {
    private static final String INCLUDEALL = "IncludeAll";
    private static final String INCLUDE = "Include";
    private static final String EXCLUDE = "Exclude";
    private static final boolean debug = false;
    private Reporter reporter;

    @Override
    public String getName() {
        return "IncludeDoclet";
    }

    @Override
    public void init(Locale locale, Reporter reporter) {
        this.reporter = reporter;
        super.init(locale, reporter);
    }

    @Override
    public boolean run(DocletEnvironment docEnv) {
        report("Running IncludeDoclet");
        return super.run(new DocletEnvironmentImplementation(docEnv));
    }

    private void report(final String report) {
        if(debug) {
            reporter.print(Diagnostic.Kind.NOTE, report);
        }
    }

    private void reportStartSection() {
        if(debug) {
            reporter.print(Diagnostic.Kind.NOTE, "----------START SECTION-----------");
        }
    }

    private void reportEndSection() {
        if(debug) {
            reporter.print(Diagnostic.Kind.NOTE, "-----------END SECTION-------------");
        }
    }

    /**
     * This wrapper extends jdk.javadoc.internal.tool. To run, compile with
     * --add-exports jdk.javadoc/jdk.javadoc.internal.tool=ALL-UNNAMED
     *
     * This will also need to be added to the javadoc command e.g.
     * javadoc -J--add-exports -Jjdk.javadoc/jdk.javadoc.internal.tool=ALL-UNNAMED -docletpath ~/dev/Javadoc/src/main/java/        -doclet IncludeDoclet  -tag Exclude:a: -tag Include:a: -tag IncludeAll:a:   -d ~/dev/Javadoc/build/javadoc   com.javadoc > output.txt
     */
    private class DocletEnvironmentImplementation extends jdk.javadoc.internal.tool.DocEnvImpl {

        private final Set<Element> includeAllClasses = new HashSet<>();
        private final Set<Element> notIncludeAllClasses = new HashSet<>();

        private DocletEnvironmentImplementation(DocletEnvironment docletEnvironment) {
            super(((DocEnvImpl) docletEnvironment).toolEnv, ((DocEnvImpl) docletEnvironment).etable);
        }

        @Override
        public Set<? extends Element> getIncludedElements() {
            final Set<? extends Element> elements = super.getIncludedElements();
            reportStartSection();
            report("Running getIncludedElements");
            report("All elements: " + elements);
            report("All element kinds: " + Arrays.toString(elements.stream().map(Element::getKind).toArray()));

            final Set<Element> ret = new LinkedHashSet<>();

            for (final Element e : elements) {
                if(isPackageOrModule(e)) {
                    ret.add(e);
                } else {
                    if(isIncludedCustomTags(e)) {
                        ret.add(e);
                    }
                }
            }

            report("All included elements: " + ret);
            report("All included element kinds: " + Arrays.toString(ret.stream().map(Element::getKind).toArray()));
            reportEndSection();

            return ret;
        }

        @Override
        public boolean isIncluded(Element e) {
            reportStartSection();
            final String prefix = "isIncluded: ";
            report("Running " + prefix + e);

            boolean include;
            if(isPackageOrModule(e)) {
                include = super.isIncluded(e);
            } else {
                include = isIncludedCustomTags(e) && super.isIncluded(e);
            }
            report(prefix + e.getKind());
            report(prefix + include);
            reportEndSection();

            return include;
        }

        @Override
        public boolean isSelected(Element e) {
            reportStartSection();
            final String prefix = "isSelected: ";
            report("Running " + prefix + e);

            boolean select;
            if(isPackageOrModule(e)) {
                select = super.isSelected(e);
            } else {
                select = isIncludedCustomTags(e) && super.isSelected(e);
            }
            report(prefix + e.getKind());
            report(prefix + select);
            reportEndSection();

            return select;
        }

        private boolean isIncludedCustomTags(final Element e) {
            final DocCommentTree docCommentTree = super.getDocTrees().getDocCommentTree(e);

            if (docCommentTree != null) {
                //The custom tags are block tags (as opposed to an inline tag {@example})
                final BlockTagTree[] trees = docCommentTree
                    .getBlockTags()
                    .stream()
                    .filter(bt -> bt.getKind() == DocTree.Kind.UNKNOWN_BLOCK_TAG)
                    .map(bt -> (BlockTagTree) bt)
                    .toArray(BlockTagTree[]::new);

                report("Doc trees for element name " + e.getSimpleName() + " of type " + e.getKind());
                report(Arrays.toString(trees));

                for (BlockTagTree dt : trees) {
                    final String tagName = dt.getTagName();
                    report("TagName " + tagName);

                    switch (tagName) {
                        case INCLUDEALL:
                            if (isClass(e)) {
                                includeAllClasses.add(e);
                            }
                            return true;

                        case INCLUDE:
                            return true;

                        case EXCLUDE:
                            return false;
                    }

                }
            }

            //nested classes must have their own include tags
            return !isClass(e) && hasIncludeAllParent(e);
        }

        private boolean isClass(Element e) {
            return e != null && e.getKind() == ElementKind.CLASS;
        }

        private boolean isIncludeAllClass(final Element e) {
            reportStartSection();
            report("Running isIncludeAllClass");
            report("Element " + e);
            report("Element kind " + e.getKind());

            if(!isClass(e)) {
                report("Not a class");
                return false;
            } else if(isClass(e) && includeAllClasses.contains(e)) {
                report("Class found in includeAllClasses");
                reportEndSection();
                return true;
            } else if(isClass(e) && notIncludeAllClasses.contains(e)) {
                report("Class found in notIncludeAllClasses");
                reportEndSection();
                return false;
            }


            //search doc to find INCLUDEALL tag
            final DocCommentTree docCommentTree = super.getDocTrees().getDocCommentTree(e);

            if (docCommentTree != null) {
                final String[] trees = docCommentTree
                    .getBlockTags()
                    .stream()
                    .filter(bt -> bt.getKind() == DocTree.Kind.UNKNOWN_BLOCK_TAG)
                    .map(bt -> ((BlockTagTree) bt).getTagName())
                    .filter(tagName -> tagName.equals(INCLUDEALL))
                    .toArray(String[]::new);

                report("Looking through block tags " + docCommentTree.getBlockTags());
                report("IncludeAllFound " + Arrays.toString(trees));
                reportEndSection();
                if(trees.length > 0) {
                    includeAllClasses.add(e);
                    return true;
                } else {
                    notIncludeAllClasses.add(e);
                    return false;
                }
            }

            report("DocCommentTree null");
            reportEndSection();
            return false;
        }

        private boolean hasIncludeAllParent(final Element e) {
            reportStartSection();
            report("Running hasIncludeAllParent");

            final Element enclosingElement = e.getEnclosingElement();

            report("EnclosingElement " + enclosingElement);
            report("EnclosingElement kind" + (enclosingElement == null ? "" : enclosingElement.getKind()));

            if(isClass(enclosingElement)) {
                final boolean hasIncludeAllParent = isIncludeAllClass(enclosingElement);
                report("hasIncludeAllParent=" + hasIncludeAllParent);
                reportEndSection();
                return hasIncludeAllParent;
            }

            report("hasIncludeAllParent=false");
            reportEndSection();
            return false;
        }

        private boolean isPackageOrModule(final Element e) {
            return e != null && (e.getKind() == ElementKind.PACKAGE || e.getKind() == ElementKind.MODULE);
        }
    }
}

