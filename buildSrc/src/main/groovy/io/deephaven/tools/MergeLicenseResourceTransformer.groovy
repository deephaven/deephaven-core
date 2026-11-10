/*
 * Ported from MergeLicenseResourceTransformer in Shadow 9.6.1 (https://github.com/GradleUp/shadow), which is
 * licensed under the Apache License, Version 2.0 (https://www.apache.org/licenses/LICENSE-2.0).
 * Modified by Deephaven Data Labs to implement the Shadow 8 Transformer interface.
 */
package io.deephaven.tools

import com.github.jengelman.gradle.plugins.shadow.transformers.CacheableTransformer
import com.github.jengelman.gradle.plugins.shadow.transformers.Transformer
import com.github.jengelman.gradle.plugins.shadow.transformers.TransformerContext
import groovy.transform.CompileStatic
import org.apache.tools.zip.ZipEntry
import org.apache.tools.zip.ZipOutputStream
import org.gradle.api.file.FileTreeElement
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity

import java.nio.charset.StandardCharsets

/**
 * A shadow jar {@link Transformer} that writes a single file made of the artifact's own license text followed by the
 * distinct texts of every included file found in the shadowed dependencies.
 *
 * <p>This follows the {@code MergeLicenseResourceTransformer} of Shadow 9, which Shadow 8 does not have: the output
 * is an optional {@code SPDX-License-Identifier} line, the contents of {@link #artifactLicense}, and then, when any
 * dependency texts were found, {@link #firstSeparator} and those texts joined by {@link #separator}.
 */
@CacheableTransformer
@CompileStatic
class MergeLicenseResourceTransformer implements Transformer {
    private final Set<String> elements = new LinkedHashSet<>()

    /** The paths, relative to the jar root, of the dependency files to merge. */
    @Input
    List<String> includes = []

    /** The path to write the merged file to. */
    @Input
    String outputPath = 'META-INF/LICENSE'

    /** When neither null nor blank, written as an {@code SPDX-License-Identifier} header line. */
    @Input
    String artifactLicenseSpdxId = 'Apache-2.0'

    /** The artifact's own license text, which starts the merged file. */
    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    File artifactLicense

    /** Written between the artifact's own text and the dependency texts. */
    @Input
    String firstSeparator = """
${'-' * 120}

This artifact includes dependencies with the following licenses:
----------------------------------------------------------------
""".toString()

    /** Written between consecutive dependency texts. */
    @Input
    String separator = "\n${'-' * 120}\n".toString()

    @Override
    boolean canTransformResource(FileTreeElement element) {
        return element.relativePath.pathString in includes
    }

    @Override
    void transform(TransformerContext context) {
        String content = trimNewlines(new String(context.is.readAllBytes(), StandardCharsets.UTF_8))
        if (!content.isEmpty()) {
            elements.add(content)
        }
    }

    @Override
    boolean hasTransformedResource() {
        return true
    }

    @Override
    void modifyOutputStream(ZipOutputStream os, boolean preserveFileTimestamps) {
        ZipEntry entry = new ZipEntry(outputPath)
        entry.time = TransformerContext.getEntryTimestamp(preserveFileTimestamps, entry.time)
        os.putNextEntry(entry)
        os.write(buildLicense().getBytes(StandardCharsets.UTF_8))
        os.closeEntry()
    }

    private String buildLicense() {
        StringBuilder sb = new StringBuilder()
        if (artifactLicenseSpdxId != null && !artifactLicenseSpdxId.isBlank()) {
            sb.append("SPDX-License-Identifier: ${artifactLicenseSpdxId}\n")
        }
        sb.append(artifactLicense.getText(StandardCharsets.UTF_8.name()))
        if (!elements.isEmpty()) {
            sb.append('\n').append(firstSeparator).append('\n')
            sb.append(elements.join('\n' + separator + '\n'))
        }
        return sb.toString()
    }

    private static String trimNewlines(String text) {
        int start = 0
        int end = text.length()
        while (start < end && isNewline(text.charAt(start))) {
            start++
        }
        while (end > start && isNewline(text.charAt(end - 1))) {
            end--
        }
        return text.substring(start, end)
    }

    private static boolean isNewline(char ch) {
        return ch == ('\n' as char) || ch == ('\r' as char)
    }
}
