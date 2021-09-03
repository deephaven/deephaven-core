package io.deephaven.engine.util.scripts;

import io.deephaven.base.FileUtils;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.util.ExceptionDetails;
import junit.framework.TestCase;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.StoredConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

public class TestScriptRepository extends BaseArrayTestCase {

    private File tempDir;
    private Path dirToImport;
    private Path repo;
    private Git git;

    @SuppressWarnings("ResultOfMethodCallIgnored")
    public void setUp() throws Exception {
        super.setUp();

        tempDir = Files.createTempDirectory("TempRepository").toFile();
        dirToImport = Paths.get(tempDir.toString(), "source");
        dirToImport.toFile().mkdir();
        repo = Paths.get(tempDir.toString(), "repo");

        Paths.get(dirToImport.toString(), "path1").toFile().mkdir();
        Paths.get(dirToImport.toString(), "path1", "a").toFile().mkdir();
        Paths.get(dirToImport.toString(), "path1", "a", "b").toFile().mkdir();
        Paths.get(dirToImport.toString(), "path1", "a", "b", "baz.groovy").toFile().createNewFile();
        Paths.get(dirToImport.toString(), "path1", "a", "b", "quux.py").toFile().createNewFile();

        Paths.get(dirToImport.toString(), "path2").toFile().mkdir();
        Paths.get(dirToImport.toString(), "path2", "c").toFile().mkdir();
        Paths.get(dirToImport.toString(), "path2", "c", "foo").toFile().createNewFile();
        Paths.get(dirToImport.toString(), "path2", "c", "foo.groovy").toFile().createNewFile();
        Paths.get(dirToImport.toString(), "path2", "c", "bar.groovY").toFile().createNewFile();

        git = Git.init().setDirectory(dirToImport.toFile()).call();
        git.add().addFilepattern("path1").call();
        git.add().addFilepattern("path2").call();
        git.commit().setMessage("Initial commit.").call();


        Git.cloneRepository().setDirectory(repo.toFile()).setURI(dirToImport.toAbsolutePath().toString()).call();

        StoredConfig config = git.getRepository().getConfig();
        config.setString("remote", "origin", "url", repo.toAbsolutePath().toString());
        config.save();

    }

    public void tearDown() throws Exception {
        super.tearDown();
        FileUtils.deleteRecursively(tempDir);
    }

    public void testGetAvailableDisplayPaths() throws IOException, GitAPIException {
        testGetAvailableScriptPaths(false, false);
    }

    public void testGetAvailableDisplayPathsWithPrefix() throws IOException, GitAPIException {
        testGetAvailableScriptPaths(true, false);
    }

    public void testGetAvailableDisplayPathsWithPrefixAndReset() throws IOException, GitAPIException {
        testGetAvailableScriptPaths(true, true);
    }

    private void testGetAvailableScriptPaths(boolean prefixDisplayPathsWithRepoName, boolean resetGitLockFiles)
            throws IOException, GitAPIException {
        StreamLoggerImpl logger = new StreamLoggerImpl(System.out, LogLevel.DEBUG);

        Path path = new File(tempDir + "/checkout").toPath();
        ScriptRepository scriptRepository = new ScriptRepository(logger, "Dummy", Collections.singleton("*"),
                repo.toAbsolutePath().toString(), true, false, "origin", "master", prefixDisplayPathsWithRepoName,
                path.toAbsolutePath(), resetGitLockFiles, Paths.get(path.toAbsolutePath().toString(), "path1"),
                Paths.get(path.toAbsolutePath().toString(), "path2"));

        Set<String> result = scriptRepository.getAvailableScriptDisplayPaths(ScriptPathLoaderState.NONE);
        ScriptPathLoaderState state = scriptRepository.getState();

        String prefix = prefixDisplayPathsWithRepoName ? "Dummy/" : "";

        Set<String> expected = new TreeSet<>();
        expected.add(prefix + "path1/a/b/baz.groovy");
        expected.add(prefix + "path1/a/b/quux.py");
        expected.add(prefix + "path2/c/bar.groovY");
        expected.add(prefix + "path2/c/foo.groovy");

        TestCase.assertEquals(expected, new TreeSet<>(result));

        // noinspection ResultOfMethodCallIgnored
        Paths.get(dirToImport.toString(), "path2", "c", "bar.groovY").toFile().delete();
        git.rm().addFilepattern("path2/c/bar.groovY").call();
        git.commit().setMessage("Dummy message").call();
        git.push().setRemote("origin").call();

        System.out.println("Checking result against old state.");

        scriptRepository.refresh();

        Set<String> result2 = scriptRepository.getAvailableScriptDisplayPaths(state);
        Set<String> result3 = scriptRepository.getAvailableScriptDisplayPaths();

        TestCase.assertEquals(expected, new TreeSet<>(result2));


        expected.remove(prefix + "path2/c/bar.groovY");
        TestCase.assertEquals(expected, new TreeSet<>(result3));
    }

    public void testGetAvailableScriptPathsWithLockfileNoReset() throws IOException {
        testGetAvailableScriptPathsWithLockfile(false);
    }

    public void testGetAvailableScriptPathsWithLockfileReset() throws IOException {
        testGetAvailableScriptPathsWithLockfile(true);
    }

    private void testGetAvailableScriptPathsWithLockfile(final boolean resetLockFile) throws IOException {
        final StreamLoggerImpl logger = new StreamLoggerImpl(System.out, LogLevel.DEBUG);

        // Need to create repo so there's a valid git in which to create a lockfile
        final Path path = new File(tempDir + "/checkout").toPath();
        new ScriptRepository(logger, "Dummy", Collections.singleton("*"), repo.toAbsolutePath().toString(), true, false,
                "origin", "master", true, path.toAbsolutePath(), false,
                Paths.get(path.toAbsolutePath().toString(), "path1"),
                Paths.get(path.toAbsolutePath().toString(), "path2"));

        final Path gitPath = path.resolve(".git");
        if (!gitPath.toFile().exists()) {
            fail("Unable to find git repo directory " + gitPath.toString());
        }

        final File lockFile = gitPath.resolve("index.lock").toFile();
        if (!lockFile.exists()) {
            new FileOutputStream(lockFile).close();
        }

        try {
            new ScriptRepository(logger, "Dummy", Collections.singleton("*"), repo.toAbsolutePath().toString(), true,
                    false, "origin", "master", true, path.toAbsolutePath(), resetLockFile,
                    Paths.get(path.toAbsolutePath().toString(), "path1"),
                    Paths.get(path.toAbsolutePath().toString(), "path2"));
            if (!resetLockFile) {
                fail("Expected exception from script repo setup");
            }
        } catch (Exception e) {
            if (resetLockFile) {
                throw e;
            }
            final String shortCauses = new ExceptionDetails(e).getShortCauses();
            if (!shortCauses.contains("Cannot lock")) {
                fail("Did not receive expected exception, instead received " + shortCauses);
            }
        }
    }
}
