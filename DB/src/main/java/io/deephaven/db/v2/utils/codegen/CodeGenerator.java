package io.deephaven.db.v2.utils.codegen;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.deephaven.db.util.IterableUtils.makeCommaSeparatedList;

public abstract class CodeGenerator {
    public static CodeGenerator create(Object... args) {
        return createSlice(args, 0);
    }

    /**
     * Open new block. Opening brace on same line.
     */
    public static CodeGenerator block(Object... args) {
        return new Block("", CodeGenerator.create(args));
    }

    /**
     * Begin new indentation scope, e.g. to format multiple lines of function parameters.
     */
    public static CodeGenerator indent(Object... args) {
        return new Indent(CodeGenerator.create(args));
    }

    /**
     * The tail wagging the dog: the proper method signature for this method is
     * {@code CodeGenerator samelineBlock(String prefix, Object... args)} But when I do that, IntelliJ by default
     * litters up the code with parameter hints, which (if the programmer doesn't turn them off), makes the templated
     * code much more unreadable. So instead we just pull out the parameter from here.
     * 
     * @param args A prefix (of type String) like "else", followed by an arbitrary number of template lines.
     * @return The new component.
     */
    public static CodeGenerator samelineBlock(Object... args) {
        final String prefix = (String) args[0];
        return new Block(prefix, CodeGenerator.createSlice(args, 1));
    }

    /**
     * Same "tail wagging the dog" comment applies.
     */
    public static CodeGenerator optional(Object... args) {
        final String tag = (String) args[0];
        return new Optional(tag, CodeGenerator.createSlice(args, 1));
    }

    /**
     * Same "tail wagging the dog" comment applies.
     */
    public static CodeGenerator repeated(Object... args) {
        final String tag = (String) args[0];
        return new Repeated(tag, CodeGenerator.createSlice(args, 1));
    }

    private static CodeGenerator createSlice(final Object[] args, final int start) {
        final CodeGenerator[] items = new CodeGenerator[args.length - start];
        for (int srcIndex = start; srcIndex < args.length; ++srcIndex) {
            final int destIndex = srcIndex - start;
            final Object o = args[srcIndex];
            if (o instanceof String) {
                items[destIndex] = new Singleton((String) o);
                continue;
            }
            if (o instanceof CodeGenerator) {
                items[destIndex] = (CodeGenerator) o;
                continue;
            }
            throw new UnsupportedOperationException(
                    "Value " + o + " is of unsupported type " + o.getClass().getSimpleName());
        }
        return new Container(items);
    }

    public final void replace(String metaVariable, String replacement) {
        final String bracketed = "[[" + metaVariable + "]]";
        final int count = replaceBracketed(bracketed, replacement);
        if (count == 0) {
            throw new UnsupportedOperationException("Couldn't find any instances of metavariable " + metaVariable);
        }
    }

    public final CodeGenerator activateOptional(String tag) {
        final List<Optional> allOptionals = new ArrayList<>();
        gatherOptionals(tag, allOptionals);
        if (allOptionals.size() == 0) {
            throw new UnsupportedOperationException("Can't find optional tag: " + tag);
        }
        if (allOptionals.size() > 1) {
            throw new UnsupportedOperationException("There are multiple instances of optional tag: " + tag);
        }
        return allOptionals.get(0).activate();
    }

    public final void activateAllOptionals(String tag) {
        final List<Optional> allOptionals = new ArrayList<>();
        gatherOptionals(tag, allOptionals);
        allOptionals.forEach(Optional::activate);
    }

    public final CodeGenerator instantiateNewRepeated(String tag) {
        final List<Repeated> allRepeateds = new ArrayList<>();
        gatherRepeateds(tag, allRepeateds);
        if (allRepeateds.size() == 0) {
            throw new UnsupportedOperationException("Can't find repeated tag: " + tag);
        }
        if (allRepeateds.size() > 1) {
            throw new UnsupportedOperationException("There are multiple instances of repeated tag: " + tag);
        }
        return allRepeateds.get(0).instantiateNew();
    }

    public final String build() {
        assertNoUnresolvedVariables();
        final StringBuilder sb = new StringBuilder();
        final String[] separatorHolder = new String[] {""};
        appendToBuilder(sb, "", separatorHolder);
        return sb.toString();
    }

    public final CodeGenerator freeze() {
        assertNoUnresolvedVariables();
        return freezeHelper();
    }

    public void assertNoUnresolvedVariables() {
        final Set<String> unresolvedVariables = new TreeSet<>();
        final Pattern p = Pattern.compile("\\[\\[.+?]]");
        findUnresolved(p, unresolvedVariables);
        if (unresolvedVariables.size() > 0) {
            throw new UnsupportedOperationException("The following variables are still unresolved: " +
                    makeCommaSeparatedList(unresolvedVariables));
        }
    }

    abstract CodeGenerator cloneMe();

    abstract CodeGenerator freezeHelper();

    abstract int replaceBracketed(String bracketed, String replacement);

    abstract void findUnresolved(Pattern pattern, Set<String> unresolved);

    abstract void gatherOptionals(String tag, final List<Optional> allOptionals);

    abstract void gatherRepeateds(String tag, List<Repeated> allRepeateds);

    abstract void appendToBuilder(StringBuilder sb, String indent, String[] separatorHolder);
}


class Container extends CodeGenerator {
    public static final Container EMPTY = new Container(new CodeGenerator[0]);

    private final CodeGenerator[] items;

    Container(final CodeGenerator[] items) {
        this.items = items;
    }

    @Override
    Container cloneMe() {
        return new Container(Arrays.stream(items).map(CodeGenerator::cloneMe).toArray(CodeGenerator[]::new));
    }

    @Override
    int replaceBracketed(final String bracketed, final String replacement) {
        int count = 0;
        for (CodeGenerator item : items) {
            count += item.replaceBracketed(bracketed, replacement);
        }
        return count;
    }

    @Override
    public void findUnresolved(final Pattern pattern, final Set<String> unresolved) {
        Arrays.stream(items).forEach(item -> item.findUnresolved(pattern, unresolved));
    }

    public void gatherOptionals(String tag, final List<Optional> allOptionals) {
        Arrays.stream(items).forEach(item -> item.gatherOptionals(tag, allOptionals));
    }

    @Override
    public void gatherRepeateds(String tag, List<Repeated> allRepeateds) {
        Arrays.stream(items).forEach(item -> item.gatherRepeateds(tag, allRepeateds));
    }

    @Override
    public void appendToBuilder(StringBuilder sb, String indent, String[] separatorHolder) {
        Arrays.stream(items).forEach(item -> item.appendToBuilder(sb, indent, separatorHolder));
    }

    @Override
    public CodeGenerator freezeHelper() {
        final CodeGenerator[] newItems =
                Arrays.stream(items).map(CodeGenerator::freezeHelper).toArray(CodeGenerator[]::new);
        return new Container(newItems);
    }
}


class Singleton extends CodeGenerator {
    private String line;

    Singleton(final String line) {
        this.line = line;
    }

    @Override
    public CodeGenerator cloneMe() {
        return new Singleton(line);
    }

    @Override
    public int replaceBracketed(String bracketed, String replacement) {
        int start = line.indexOf(bracketed);
        if (start < 0) {
            return 0;
        }
        final StringBuilder sb = new StringBuilder();
        int previousEnd = 0;
        int numMatches = 0;
        while (start >= 0) {
            sb.append(line, previousEnd, start);
            sb.append(replacement);
            previousEnd = start + bracketed.length();
            start = line.indexOf(bracketed, previousEnd);
            ++numMatches;
        }
        sb.append(line, previousEnd, line.length());
        line = sb.toString();
        return numMatches;
    }

    @Override
    public void findUnresolved(Pattern pattern, Set<String> unresolved) {
        final Matcher m = pattern.matcher(line);
        while (m.find()) {
            unresolved.add(m.group());
        }
    }

    @Override
    public void gatherOptionals(String tag, List<Optional> allOptionals) {
        // Do nothing
    }

    @Override
    public void gatherRepeateds(String tag, List<Repeated> allRepeateds) {
        // Do nothing
    }

    @Override
    public void appendToBuilder(StringBuilder sb, String indent, String[] separatorHolder) {
        sb.append(separatorHolder[0]);
        // Empty lines should not be indented
        if (!line.isEmpty()) {
            sb.append(indent);
            sb.append(line);
        }
        separatorHolder[0] = "\n";
    }

    @Override
    public CodeGenerator freezeHelper() {
        return new Singleton(line);
    }
}


final class Block extends CodeGenerator {
    /**
     * The {@code prefix} is used for special indentation, e.g. in the "else" part of an if-else statement, or the
     * "catch" part of a try-catch block.
     */
    private final String prefix;
    private final CodeGenerator inner;

    Block(final String prefix, final CodeGenerator inner) {
        this.prefix = prefix;
        this.inner = inner;
    }

    @Override
    public CodeGenerator cloneMe() {
        return new Block(prefix, inner.cloneMe());
    }

    @Override
    public int replaceBracketed(String bracketed, String replacement) {
        return inner.replaceBracketed(bracketed, replacement);
    }

    @Override
    public void findUnresolved(Pattern pattern, Set<String> unresolved) {
        inner.findUnresolved(pattern, unresolved);
    }

    @Override
    public void gatherOptionals(String tag, List<Optional> allOptionals) {
        inner.gatherOptionals(tag, allOptionals);
    }

    @Override
    public void gatherRepeateds(String tag, List<Repeated> allRepeateds) {
        inner.gatherRepeateds(tag, allRepeateds);
    }

    @Override
    public void appendToBuilder(StringBuilder sb, String indent, String[] separatorHolder) {
        if (!prefix.isEmpty()) {
            sb.append(' ');
            sb.append(prefix);
        }
        sb.append(" {");
        inner.appendToBuilder(sb, indent + "    ", separatorHolder);
        sb.append(separatorHolder[0]);
        sb.append(indent);
        sb.append('}');
        separatorHolder[0] = "\n";
    }

    @Override
    public CodeGenerator freezeHelper() {
        return new Block(prefix, inner.freezeHelper());
    }
}


final class Indent extends CodeGenerator {
    private final CodeGenerator inner;

    Indent(final CodeGenerator inner) {
        this.inner = inner;
    }

    @Override
    public CodeGenerator cloneMe() {
        return new Indent(inner.cloneMe());
    }

    @Override
    public int replaceBracketed(String bracketed, String replacement) {
        return inner.replaceBracketed(bracketed, replacement);
    }

    @Override
    public void findUnresolved(Pattern pattern, Set<String> unresolved) {
        inner.findUnresolved(pattern, unresolved);
    }

    @Override
    public void gatherOptionals(String tag, List<Optional> allOptionals) {
        inner.gatherOptionals(tag, allOptionals);
    }

    @Override
    public void gatherRepeateds(String tag, List<Repeated> allRepeateds) {
        inner.gatherRepeateds(tag, allRepeateds);
    }

    @Override
    public void appendToBuilder(StringBuilder sb, String indent, String[] separatorHolder) {
        // Arbitrarily decide to indent by 8
        inner.appendToBuilder(sb, indent + "        ", separatorHolder);
    }

    @Override
    public CodeGenerator freezeHelper() {
        return new Indent(inner.freezeHelper());
    }
}


final class Optional extends CodeGenerator {
    private final String tag;
    private final CodeGenerator inner;
    private boolean active;

    Optional(String tag, CodeGenerator inner) {
        this(tag, inner, false);
    }

    private Optional(String tag, CodeGenerator inner, boolean active) {
        this.tag = tag;
        this.inner = inner;
        this.active = active;
    }

    @Override
    public CodeGenerator cloneMe() {
        return new Optional(tag, (CodeGenerator) inner.cloneMe(), active);
    }

    @Override
    public int replaceBracketed(String bracketed, String replacement) {
        return active ? inner.replaceBracketed(bracketed, replacement) : 0;
    }

    @Override
    public void findUnresolved(Pattern pattern, Set<String> unresolved) {
        if (active) {
            inner.findUnresolved(pattern, unresolved);
        }
    }

    @Override
    public void gatherOptionals(String tag, List<Optional> allOptionals) {
        if (tag.equals(this.tag)) {
            allOptionals.add(this);
        }
        inner.gatherOptionals(tag, allOptionals);
    }

    @Override
    public void gatherRepeateds(String tag, List<Repeated> allRepeateds) {
        inner.gatherRepeateds(tag, allRepeateds);
    }

    @Override
    public void appendToBuilder(StringBuilder sb, String indent, String[] separatorHolder) {
        if (active) {
            inner.appendToBuilder(sb, indent, separatorHolder);
        }
    }

    public CodeGenerator activate() {
        active = true;
        return inner;
    }

    public String tag() {
        return tag;
    }

    @Override
    public CodeGenerator freezeHelper() {
        return active ? inner : Container.EMPTY;
    }
}


final class Repeated extends CodeGenerator {
    private final String tag;
    private final CodeGenerator prototype;
    private final List<CodeGenerator> instances;

    Repeated(String tag, CodeGenerator prototype) {
        this(tag, prototype, new ArrayList<>());
    }

    Repeated(String tag, CodeGenerator prototype, List<CodeGenerator> instances) {
        this.tag = tag;
        this.prototype = prototype;
        this.instances = instances;
    }

    public CodeGenerator instantiateNew() {
        final CodeGenerator result = prototype.cloneMe();
        instances.add(result);
        return result;
    }

    @Override
    public CodeGenerator cloneMe() {
        final List<CodeGenerator> newInstances = new ArrayList<>();
        final CodeGenerator newPrototype = prototype.cloneMe();
        for (final CodeGenerator instance : instances) {
            newInstances.add(instance.cloneMe());
        }
        return new Repeated(tag, newPrototype, newInstances);
    }

    @Override
    public int replaceBracketed(String bracketed, String replacement) {
        // Not 100% sure what to do here, but I think I will replace everything in the prototype and
        // currently-instantiated instances.
        int count = prototype.replaceBracketed(bracketed, replacement);
        for (final CodeGenerator instance : instances) {
            count += instance.replaceBracketed(bracketed, replacement);
        }
        return count;
    }

    @Override
    public void findUnresolved(Pattern pattern, Set<String> unresolved) {
        instances.forEach(instance -> instance.findUnresolved(pattern, unresolved));
    }

    @Override
    public void gatherOptionals(String tag, List<Optional> allOptionals) {
        prototype.gatherOptionals(tag, allOptionals);
        instances.forEach(instance -> instance.gatherOptionals(tag, allOptionals));
    }

    @Override
    public void gatherRepeateds(String tag, List<Repeated> allRepeateds) {
        if (tag.equals(this.tag)) {
            allRepeateds.add(this);
        }
        prototype.gatherRepeateds(tag, allRepeateds);
        instances.forEach(instance -> instance.gatherRepeateds(tag, allRepeateds));
    }

    @Override
    public void appendToBuilder(StringBuilder sb, String indent, String[] separatorHolder) {
        for (final CodeGenerator instance : instances) {
            instance.appendToBuilder(sb, indent, separatorHolder);
        }
    }

    @Override
    public CodeGenerator freezeHelper() {
        final CodeGenerator[] frozen =
                instances.stream().map(CodeGenerator::freezeHelper).toArray(CodeGenerator[]::new);
        return new Container(frozen);
    }

    public String tag() {
        return tag;
    }
}
