/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.files;

import io.deephaven.configuration.Configuration;
import org.apache.tools.ant.DirectoryScanner;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ResourceResolution {
    private Set<String> resourceList = null; // Duplicates are not allowed
    private final static String DEFAULT_SPLIT_REGEX = "[ ;]+";

    // To normalize file paths for wildcard searches
    private final static Pattern normalizePattern = Pattern.compile("([\\\\/]+)");

    /**
     * An instance of this class should be created to contain the required parameters to use for the resource search
     *
     * @param configuration configuration to be used for resource resolution
     * @param delimiterRegex regular expression to be used to delimit the resources; if it is null or has a length of 0
     *        then the default (semicolon or space) will be used
     * @param delimitedResourceList list of resources to find, delimited by delimiterRegex. These resources can include
     *        files and directories. There can be many of these.
     */
    public ResourceResolution(@NotNull final Configuration configuration, final String delimiterRegex,
            @NotNull final String... delimitedResourceList) {
        final Pattern delimiterPattern = Pattern
                .compile(delimiterRegex == null || delimiterRegex.isEmpty() ? DEFAULT_SPLIT_REGEX : delimiterRegex);
        resourceList = Arrays.stream(delimitedResourceList)
                .filter(l -> l != null && !l.isEmpty())
                .flatMap(l -> Arrays.stream(delimiterPattern.split(l)))
                .filter(r -> !r.isEmpty())
                .map(r -> {
                    String subs = r.contains("<devroot>") ? r.replace("<devroot>", configuration.getDevRootPath()) : r;
                    subs = subs.contains("<workspace>") ? subs.replace("<workspace>", configuration.getWorkspacePath())
                            : subs;
                    return subs;
                })
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private void processJarFile(@NotNull final Path path, @NotNull final String suffix,
            @NotNull final BiConsumer<URL, String> consumer) throws IOException {
        /*
         * Wrap all this in a try block, as we don't want to fail if there's a matching filename that's not a jar. Is
         * there a better way to handle this?
         */
        final URL baseUrl = path.toUri().toURL();
        try (final JarFile jarFile = new JarFile(path.toFile())) {
            final Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                final JarEntry jarEntry = entries.nextElement();
                String nameInJarFile = jarEntry.getName();
                if (nameInJarFile.endsWith(suffix)) {
                    final String[] fileNameSplit = nameInJarFile.split("/");
                    // The URL needs a leading slash in the name or it won't build correctly.
                    if (!nameInJarFile.startsWith("/")) {
                        nameInJarFile = "/" + nameInJarFile;
                    }
                    final URL fullUrl = new URL("jar:" + baseUrl.toString() + "!" + nameInJarFile);
                    consumer.accept(fullUrl, fileNameSplit[fileNameSplit.length - 1]);
                }
            }
        }
    }

    /**
     * Find all resources associated with a list of resources
     * 
     * @param suffix filename suffix to be searched for
     * @param consumer operation to perform on each found resource (URL, file name pair)
     * @throws IOException in the event of issues opening files or malformed URLs; this should not occur unless files
     *         are being modified as we walk the directories
     */
    public void findResources(@NotNull final String suffix, @NotNull final BiConsumer<URL, String> consumer)
            throws IOException {
        if (resourceList.isEmpty()) {
            return;
        }

        int traversedPaths = 0;
        for (final String resourcePath : resourceList) {

            // If the resource doesn't exist, it may be a wildcard entry so try a wildcard search
            if (!(new File(resourcePath).exists())) {
                final DirectoryScanner scanner = new DirectoryScanner();

                /*
                 * Wildcards work best if we normalize the path sent in, removing extra slashes/backslashes and making
                 * everything use one separator based on the OS separator. We have to take into account full paths and
                 * relative paths on both Linux and Windows.
                 */
                final String normalizedResourcePath = normalize(resourcePath);
                final String rootPath = getRoot(normalizedResourcePath);
                scanner.setBasedir(rootPath);

                // If this resource's normalized search path starts with rootPath, then it was a fully-specified path,
                // so pull
                // that part off the path to determine the wildcard include list. Otherwise it was relative, and that
                // becomes the include array.
                final String[] wildcardIncludeAry = new String[] {normalizedResourcePath.startsWith(rootPath)
                        ? normalizedResourcePath.substring(rootPath.length())
                        : normalizedResourcePath};
                scanner.setIncludes(wildcardIncludeAry);

                scanner.setCaseSensitive(true);
                scanner.scan();
                final String[] files = scanner.getIncludedFiles();

                if (files.length > 0) {
                    traversedPaths++;

                    // The returned file array will be the relative path from the root, so add that root back and
                    // process the file
                    for (final String file : files) {
                        // The returned filename doesn't include the root, so add it
                        final String fullFilename = rootPath + File.separator + file;
                        final Path fullPath = Paths.get(fullFilename);
                        processFile(fullPath.getFileName().toString(), fullPath, suffix, consumer);
                    }
                }
            } else {
                traversedPaths++;
                try (final Stream<Path> pathStream =
                        Files.walk(Paths.get(resourcePath), FileVisitOption.FOLLOW_LINKS)) {
                    // Walk this file (or all files in this directory and subdirectories) and look for <.suffix> and
                    // .jar files.
                    pathStream
                            .filter(p -> Files.isRegularFile(p))
                            .forEach(p -> processFile(p.getFileName().toString(), p, suffix, consumer));
                } catch (UncheckedIOException e) {
                    throw e.getCause();
                }
            }
        }
        if (traversedPaths == 0) {
            throw new IOException("None of the specified resource paths exist: " + resourceList);
        }
    }

    /**
     * Find the filesystem root for this search path. For example, it may be /, C:\, or something passed by the user.
     * 
     * @param searchPath the already-normalized search path
     * @return the root for searchPath
     */
    private static String getRoot(String searchPath) {
        // Create a File, which will be used to get the fully-qualified name of this search path.
        File dir = new File(searchPath).getAbsoluteFile();
        final String fullNameWithDir = dir.getPath();

        // If we were passed an absolute path then the fully-qualified name will match the created File's name. In that
        // case
        // the root needs to be the filesystem/disk root, so look up the path until it's found. If this is a UNC path
        // we need to stop traversing when we reach the share level.
        if (new File(searchPath).isAbsolute()) {
            if (searchPath.startsWith("\\\\")) {
                File parent = dir;
                while ((parent != null)
                        && (parent.getParentFile() != null)
                        && (parent.getParentFile().getParentFile()) != null) {
                    dir = parent;
                    parent = dir.getParentFile();
                }
                return dir.getAbsolutePath();

            } else {
                File parent = dir.getParentFile();
                while (parent != null) {
                    dir = parent;
                    parent = dir.getParentFile();
                }
                return dir.getAbsolutePath();
            }
        } else {
            // Any trailing separators foil the endsWith call below. The string is supposed to be normalized, so just
            // find File.separator.
            while (searchPath.endsWith(File.separator)) {
                searchPath = searchPath.substring(0, searchPath.length() - 1);
            }

            // If it's a relative path, the root will be the part before the passed-in searchPath. as that's before the
            // wildcards could start
            if (!fullNameWithDir.endsWith(searchPath)) {
                throw new IllegalArgumentException(
                        "Can't resolve relative path " + searchPath + " to full directory name " + fullNameWithDir);
            } else {
                return fullNameWithDir.substring(0, fullNameWithDir.length() - searchPath.length());
            }
        }
    }

    private void processFile(final String fileName, final Path path, @NotNull final String suffix,
            @NotNull final BiConsumer<URL, String> consumer) {
        try {
            if (fileName.endsWith(suffix)) {
                consumer.accept(path.toUri().toURL(), fileName);
            } else if (fileName.endsWith(".jar")) {
                processJarFile(path, suffix, consumer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Replace multiple sequential file separators (either Windows or Unix) with single instances of the system file
     * separator. Make sure UNC paths are preserved by leaving two leading file sparator characters in place
     *
     * @param resourcePath the path to normalize
     * @return the normalized path
     */
    protected String normalize(final String resourcePath) {
        return ((File.separator.equals("\\") && resourcePath.startsWith("\\\\")) ? "\\" : "")
                + normalizePattern.matcher(resourcePath).replaceAll(Matcher.quoteReplacement(File.separator));
    }
}
