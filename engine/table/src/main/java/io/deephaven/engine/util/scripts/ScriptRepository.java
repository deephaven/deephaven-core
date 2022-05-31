/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util.scripts;

import io.deephaven.base.clock.TimeConstants;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.FunctionalInterfaces;
import org.apache.commons.lang3.mutable.MutableObject;
import org.eclipse.jgit.api.*;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevTree;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.OrTreeFilter;
import org.eclipse.jgit.treewalk.filter.PathFilter;
import org.eclipse.jgit.treewalk.filter.TreeFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>
 * A {@link ScriptPathLoader} that loads scripts from a git repository.
 * </p>
 * <p>
 * If this class is created with updateEnabled = false it loads scripts as if no git repository was present.
 * </p>
 */
public class ScriptRepository implements ScriptPathLoader {
    private static final int gitHashDisplayLength =
            Configuration.getInstance().getIntegerWithDefault("ScriptRepository.githashdisplaylenth", 8);
    private static final String[] scriptExtensions = ScriptExtensionsMap.getInstance().values().stream()
            .flatMap(List::stream).map(x -> "." + x.toLowerCase()).toArray(String[]::new);

    private final Logger log;
    private final String name;
    private final Set<String> groupNames;
    private final boolean prefixDisplayPathsWithRepoName;
    private final boolean gcEnabled;
    private final Path rootPath;

    private final String remoteOrigin;
    private final String logPrefix;
    private final @Nullable Git git;
    private final String upstreamBranch;
    private final ScriptFileVisitor[] searchPathVisitors;

    private final ReadWriteLock consistencyLock = new ReentrantReadWriteLock();

    private final Map<String, Path> displayPathStringToScript = new HashMap<>();
    private final Map<String, Path> relativePathStringToScript = new HashMap<>();
    private final TreeFilter scriptTreeFilter;

    private long lastGitGc;
    private long lastGitRefresh = 0;

    private static class GitState implements ScriptPathLoaderState {
        final String repoName;
        final String branch;
        final ObjectId revision;

        GitState(@NotNull final String repoName, final String branch, final ObjectId revision) {
            this.repoName = Require.neqNull(repoName, "repoName");
            this.branch = branch;
            this.revision = revision;
        }

        @Override
        public String toAbbreviatedString() {
            return repoName + ':' + revisionString();
        }

        @NotNull
        private String revisionString() {
            if (revision == null) {
                return "(unknown revision)";
            }
            return revision.abbreviate(gitHashDisplayLength).name();
        }

        @Override
        public String toString() {
            return repoName + '@' + branch + ':' + revisionString();
        }
    }

    /**
     * Constructs the script repository instance.
     * <p>
     *
     * Note that in the case of the Controller, a misconfiguration of a script repository (which will result in an
     * exception to be thrown) will cause the Controller to fail to start. This is intentional: the Controller
     * configuration is an all-or-nothing thing, and attempts to limp along could cause a misconfigured Controller to
     * stay unnoticed for weeks, and would take days to correct.
     *
     * @throws RuntimeException if the repository configuration is incorrect.
     */
    // TODO: Move most/all of the repo configuration into ACLs, or add runtime re-configuration some other way.
    ScriptRepository(@NotNull final Logger log,
            @NotNull final String name,
            @NotNull final Set<String> groupNames,
            @NotNull final String gitURI,
            final boolean updateEnabled,
            final boolean gcEnabled,
            @NotNull final String remoteOrigin,
            @Nullable final String branch,
            final boolean prefixDisplayPathsWithRepoName,
            @NotNull final Path rootPath,
            final boolean resetGitLockFiles,
            @NotNull final Path... searchPaths) {
        this.log = log;
        this.name = name;
        this.groupNames =
                groupNames == CollectionUtil.UNIVERSAL_SET ? groupNames : Collections.unmodifiableSet(groupNames);
        this.prefixDisplayPathsWithRepoName = prefixDisplayPathsWithRepoName;
        this.gcEnabled = gcEnabled;
        this.rootPath = rootPath;
        this.remoteOrigin = remoteOrigin;
        this.lastGitGc = 0;
        this.logPrefix = ScriptRepository.class.getSimpleName() + '-' + name + ": ";

        Collection<TreeFilter> scriptSuffixFilters = new ArrayList<>();
        for (String extension : scriptExtensions) {
            /*
             * Case insensitive suffix matcher.
             */
            TreeFilter caseInsensitive = new TreeFilter() {
                private final String suffix = extension.toLowerCase();

                @Override
                public boolean include(TreeWalk walker) {
                    return walker.isSubtree() || walker.getNameString().toLowerCase().endsWith(suffix);
                }

                @Override
                public boolean shouldBeRecursive() {
                    return true;
                }

                @Override
                public TreeFilter clone() {
                    // there is no mutable state in this implementation
                    return this;
                }
            };
            scriptSuffixFilters.add(caseInsensitive);
        }
        this.scriptTreeFilter = OrTreeFilter.create(scriptSuffixFilters);

        upstreamBranch = remoteOrigin + "/" + branch;
        searchPathVisitors = new ScriptFileVisitor[searchPaths.length];
        for (int spi = 0; spi < searchPaths.length; ++spi) {
            searchPathVisitors[spi] = new ScriptFileVisitor(searchPaths[spi]);
        }

        this.git = setUpGitRepository(updateEnabled, gitURI, branch, resetGitLockFiles);

        try {
            scanFileTree();
        } catch (IOException e) {
            throw new UncheckedIOException(logPrefix + "error initializing script paths", e);
        }
    }

    /**
     * Sets up the remote Git repository if updates are enabled.
     *
     * @param updateEnabled {@code true} if updates are to be enabled.
     * @param gitURI URI of the remote repository.
     * @param branch Branch to use in the remote repository.
     * @param resetGitLockFiles if true, and an index lock file (index.lock) exists, delete it
     * @return The Git repository, or {@code null} if Git updates aren't enabled.
     * @throws RuntimeException if the setup failed.
     */
    private @Nullable Git setUpGitRepository(final boolean updateEnabled,
            final String gitURI,
            final String branch,
            final boolean resetGitLockFiles) {
        if (!updateEnabled) {
            return null;
        }

        try {
            final Path gitPath = rootPath.resolve(".git");
            if (Files.isDirectory(gitPath)) {
                if (resetGitLockFiles) {
                    final Path lockFile = gitPath.resolve("index.lock");
                    if (Files.isRegularFile(lockFile)) {
                        final String lockFileName = lockFile.toAbsolutePath().toString();
                        log.warn().append("Deleting lock file ").append(lockFileName).endl();
                        try {
                            Files.delete(lockFile);
                        } catch (IOException e) {
                            throw new IOException("Unable to delete git lock file " + lockFileName, e);
                        }
                    }
                }

                Git tempGit = Git.open(rootPath.toFile());
                final Repository gitRepo = tempGit.getRepository();
                final RepositoryState gitRepoState = gitRepo.getRepositoryState();
                if (gitRepoState != RepositoryState.SAFE) {
                    throw new IllegalStateException(logPrefix
                            + "repository is not in expected state (SAFE), instead state is: " + gitRepoState);
                }

                try {
                    tempGit.fetch().setRemote(remoteOrigin).setRemoveDeletedRefs(true).call();
                    lastGitRefresh = System.currentTimeMillis();
                } catch (final Exception ex) {
                    log.warn().append(logPrefix)
                            .append("Initial git fetch failed, but repository was cloned, continuing. ").append(ex)
                            .endl();
                }

                final List<Ref> localBranches = tempGit.branchList().call();
                final String branchRefName = "refs/heads/" + branch;
                boolean needCreate = true;
                for (final Ref localBranch : localBranches) {
                    if (localBranch.getName().equals(branchRefName)) {
                        needCreate = false;
                        break;
                    }
                }

                final CheckoutCommand checkoutCommand = tempGit.checkout().setName(branch).setCreateBranch(needCreate)
                        .setForce(true).setUpstreamMode(CreateBranchCommand.SetupUpstreamMode.SET_UPSTREAM)
                        .setStartPoint(upstreamBranch);
                checkoutCommand.call();
                final CheckoutResult checkoutResult = checkoutCommand.getResult();
                if (checkoutResult.getStatus() != CheckoutResult.Status.OK) {
                    throw new IllegalStateException(
                            logPrefix + "checkout of branch " + branch + " failed: " + checkoutResult);
                }

                tempGit.reset().setMode(ResetCommand.ResetType.HARD).setRef(upstreamBranch).call();
                return tempGit;
            } else {
                String uriToClone;

                if (!new File(gitURI).exists() && !gitURI.contains("@")) {
                    // add user for old style repos, GitLab is always git@
                    uriToClone = System.getProperty("user.name") + "@" + gitURI;
                } else {
                    uriToClone = gitURI;
                }

                return Git.cloneRepository().setBranch(branch).setURI(uriToClone).setDirectory(rootPath.toFile())
                        .call();
            }
        } catch (Exception e) {
            throw new RuntimeException(logPrefix + "error setting up git repository", e);
        }
    }

    /**
     * Get the name of this repository defined by the <b>[prefix].scripts.repos property</b>.
     *
     * @return The name of this repo.
     */
    public String getName() {
        return name;
    }

    /**
     * Get the users allowed to access this repo, defined by the <b>[prefix].scripts.repo.[name].users</b> property.
     *
     * @return The names of all users allowed to access the repo.
     */
    @SuppressWarnings("WeakerAccess")
    public Set<String> getGroupNames() {
        return groupNames;
    }

    /**
     * Get a {@link ScriptPathLoaderState state} object that represents the current branch HEAD commit.
     *
     * @return The current branch HEAD or null if updates were disabled.
     */
    @Override
    public ScriptPathLoaderState getState() {
        if (git == null) {
            return ScriptPathLoaderState.NONE;
        }

        try {
            return new GitState(name, upstreamBranch, getCurrentRevision());
        } catch (IOException e) {
            throw new UncheckedIOException("Unable to get ref Id for " + name + ": " + upstreamBranch, e);
        }
    }

    /**
     * Get the ObjectId revision for the current branch's HEAD.
     *
     * @return An ObjectId for the HEAD commit.
     *
     * @throws IOException If the repo was unable to resolve the commit.
     */
    private ObjectId getCurrentRevision() throws IOException {
        return (git == null) ? null : git.getRepository().resolve(Constants.HEAD);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Path update helpers
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * <p>
     * Scan all of the configured search paths for scripts and updates the current loader state.
     * </p>
     * <p>
     * Search paths are configured via the <b>[prefix].scripts.repo.[name].paths</b> property.
     * </p>
     *
     * @throws IOException If there was a problem scanning for script files.
     */
    private void scanFileTree() throws IOException {
        displayPathStringToScript.clear();
        relativePathStringToScript.clear();
        log.info().append(logPrefix).append("beginning file tree scans").endl();

        for (final ScriptFileVisitor searchPathVisitor : searchPathVisitors) {
            if (log.isDebugEnabled()) {
                log.debug().append(logPrefix).append("searching ").append(searchPathVisitor.searchPath.toString())
                        .endl();
            }

            try {
                Files.walkFileTree(searchPathVisitor.searchPath, searchPathVisitor);
            } catch (IOException e) {
                throw new IOException(logPrefix + "Error while searching " + searchPathVisitor.searchPath.toString(),
                        e);
            }
        }

        log.info().append(logPrefix).append("finished file tree scans").endl();
    }

    private class ScriptFileVisitor extends SimpleFileVisitor<Path> {

        private final Path searchPath;

        private ScriptFileVisitor(@NotNull final Path searchPath) {
            this.searchPath = searchPath;
        }

        @Override
        public FileVisitResult visitFile(final Path file,
                final BasicFileAttributes attrs) throws IOException {
            final FileVisitResult result = super.visitFile(file, attrs);
            if (result == FileVisitResult.CONTINUE && Arrays.stream(scriptExtensions)
                    .anyMatch(extension -> file.getFileName().toString().toLowerCase().endsWith(extension))) {
                final String displayPathString = ((prefixDisplayPathsWithRepoName ? name + File.separator : "")
                        + rootPath.relativize(file).toString()).replace('\\', '/');

                if (log.isDebugEnabled()) {
                    log.debug().append(logPrefix).append("adding script path: display=").append(displayPathString)
                            .append(", absolute=").append(file.toString()).endl();
                }

                displayPathStringToScript.put(displayPathString, file);
                final String relativePathString = searchPath.relativize(file).toString().replace('\\', '/');
                relativePathStringToScript.putIfAbsent(relativePathString, file);
            }
            return result;
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ScriptPathLoader implementation
    // -----------------------------------------------------------------------------------------------------------------

    @Override
    public void lock() {
        consistencyLock.readLock().lock();
    }

    @Override
    public void unlock() {
        consistencyLock.readLock().unlock();
    }

    @Override
    public Set<String> getAvailableScriptDisplayPaths() {
        return Collections.unmodifiableSet(displayPathStringToScript.keySet());
    }

    /**
     * Get the body of a script file via the display path.
     *
     * @param displayPath The display path to load a script for.
     *
     * @return A String representing the body of the script.
     * @throws IOException If the file is not accessible.
     */
    @Override
    public String getScriptBodyByDisplayPath(@NotNull final String displayPath) throws IOException {
        lock();
        try {
            return getScriptBody(displayPathStringToScript.get(displayPath));
        } finally {
            unlock();
        }
    }

    /**
     * Get the body of a script file via the relative path.
     *
     * @param relativePath The relative path to load a script for.
     *
     * @return A String representing the body of the script
     * @throws IOException If the file is not accessible
     */
    @Override
    public String getScriptBodyByRelativePath(@NotNull final String relativePath) throws IOException {
        lock();
        try {
            return getScriptBody(relativePathStringToScript.get(relativePath));
        } finally {
            unlock();
        }
    }

    /**
     * Get the body of a script at the specified location.
     * 
     * @implNote Calls to this method should be performed after acquiring any needed locks.
     *
     * @param path The location to look for the script
     *
     * @return A String representing the body of the script.
     * @throws IOException If the file is not accessible
     */
    private static String getScriptBody(final Path path) throws IOException {
        if (path == null) {
            return null;
        }

        final byte[] contents = Files.readAllBytes(path);
        return new String(contents, 0, contents.length);
    }

    @Override
    public Set<String> getAvailableScriptDisplayPaths(final ScriptPathLoaderState state) throws IOException {
        final Set<String> items = new HashSet<>();

        this.doTreeWalk(() -> items.addAll(getAvailableScriptDisplayPaths()),
                (repository, objectId, treeWalk) -> items
                        .add((prefixDisplayPathsWithRepoName ? name + File.separator : "") + treeWalk.getPathString()),
                scriptTreeFilter, state);

        return items;
    }

    @Override
    public String getScriptBodyByRelativePath(final String relativePath, final ScriptPathLoaderState state)
            throws IOException {
        return getScriptBodyByCommit(relativePathStringToScript.get(relativePath), state);
    }

    @Override
    public String getScriptBodyByDisplayPath(final String displayPath, final ScriptPathLoaderState state)
            throws IOException {
        return getScriptBodyByCommit(displayPathStringToScript.get(displayPath), state);
    }

    /**
     * Use get the script body for a specific commit using a git {@link TreeWalk}.
     *
     * @implNote If the specified commit is the same as the current HEAD, this will go to the filesystem instead of
     *           performing the tree walk.
     *
     * @param path The absolute path to the file.
     * @param state The state containing the commit details.
     *
     * @return The body of the requested script, or null if it did not exist.
     *
     * @throws IOException If there was a problem reading the file.
     */
    private String getScriptBodyByCommit(final Path path, final ScriptPathLoaderState state) throws IOException {
        if (path == null) {
            return null;
        }

        final MutableObject<String> resultHolder = new MutableObject<>();

        // TreeWalk paths must be relative to the repo or they won't work.
        final Path repoPath = rootPath.relativize(path);
        lock();
        try {
            this.doTreeWalk(() -> resultHolder.setValue(getScriptBody(path)),
                    (repository, objectId, treeWalk) -> {
                        final ObjectLoader loader = repository.open(objectId);

                        // Then grab the contents of the object found
                        final byte[] contents = loader.getBytes();
                        resultHolder.setValue(new String(contents, 0, contents.length));
                    }, PathFilter.create(repoPath.toString()), state);

            return resultHolder.getValue();
        } finally {
            unlock();
        }
    }

    /**
     * Perform a tree walk of the specified commit, using a {@link TreeFilter filter}.
     *
     * @param fallback The method to call if There is no state, or the requested commit is the same as HEAD.
     * @param objectConsumer A consumer to handle the individual matches of the walk.
     * @param filter The filter to use to match items during the walk.
     * @param state The state object containing the commit information.
     * @param <E> An optional exception type thrown by one of the input methods.
     *
     * @throws E If one of the input methods throws E.
     * @throws IOException If there was a problem during the tree walk.
     */
    private <E extends Exception> void doTreeWalk(final FunctionalInterfaces.ThrowingRunnable<IOException> fallback,
            final FunctionalInterfaces.ThrowingTriConsumer<Repository, ObjectId, TreeWalk, E> objectConsumer,
            final TreeFilter filter,
            final ScriptPathLoaderState state) throws E, IOException {

        // If we are not actually using git, the requested commit is blank, or the default state, go ahead and invoke
        // the fallback method
        if ((git == null) || (state == null)) {
            fallback.run();
            return;
        }

        // If the state object isn't a GitState then something bad(tm) happened
        if (!(state instanceof GitState)) {
            throw new IllegalArgumentException(
                    "Repo state (" + state.getClass().getName() + ") is incorrect for ScriptRepository");
        }

        final GitState gs = (GitState) state;

        // If the requested revision is the same as the current HEAD, use the fallback method
        if (gs.revision.equals(getCurrentRevision())) {
            fallback.run();
            return;
        }

        final Repository repository = git.getRepository();
        final RevWalk revWalk = new RevWalk(repository);

        try {
            final RevCommit commit = revWalk.parseCommit(gs.revision);
            final RevTree tree = commit.getTree();
            final TreeWalk treeWalk = new TreeWalk(repository);

            // Apply Commit, and filter then perform the walk
            treeWalk.addTree(tree);
            treeWalk.setRecursive(true);
            treeWalk.setFilter(filter);

            while (treeWalk.next()) {
                objectConsumer.accept(repository, treeWalk.getObjectId(0), treeWalk);
            }
        } finally {
            revWalk.dispose();
        }
    }

    @Override
    public void refresh() {
        if (git == null) {
            return;
        }

        try {
            if (gcEnabled && ((lastGitGc + TimeConstants.DAY) < System.currentTimeMillis())) {
                log.info().append(logPrefix).append("git gc took place more than 24 hours ago").endl();
                try {
                    git.gc().call();
                    lastGitGc = System.currentTimeMillis();
                } catch (GitAPIException e) {
                    log.warn().append(logPrefix).append("error calling git gc: ").append(e).endl();
                }
                log.info().append(logPrefix).append("git gc complete").endl();
            }

            log.info().append(logPrefix).append("starting fetch").endl();
            try {
                git.fetch().setRemote(remoteOrigin).setRemoveDeletedRefs(true).call();
            } catch (GitAPIException e) {
                log.warn().append(logPrefix).append("error fetching from git: ").append(e).endl();
                return;
            }

            log.info().append(logPrefix).append("resetting to fetched head").endl();
            consistencyLock.writeLock().lock();
            try {
                git.reset().setMode(ResetCommand.ResetType.HARD).setRef(upstreamBranch).call();
                scanFileTree();
            } catch (GitAPIException e) {
                log.warn().append(logPrefix).append("error resetting git repository: ").append(e).endl();
                return;
            } catch (IOException e) {
                log.warn().append(logPrefix).append("error refreshing script paths: ").append(e).endl();
                return;
            } finally {
                consistencyLock.writeLock().unlock();
            }

            log.info().append(logPrefix).append("Successful git run after ")
                    .append(System.currentTimeMillis() - lastGitRefresh).append("ms").endl();
            lastGitRefresh = System.currentTimeMillis();
        } catch (Exception e) {
            // We are overly cautious here, to make sure that a failure to run the repository doesn't crash
            // the running process (in particular if it's the Controller).
            log.error().append(logPrefix).append("error refreshing repository: ").append(e).endl();
        }
    }

    @Override
    public void close() {
        if (git != null) {
            git.close();
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Factory methods
    // -----------------------------------------------------------------------------------------------------------------

    private static Path normalizeRootPath(final Configuration config, final String rootPathString) {
        final Path propertyRootPath = Paths.get(rootPathString);
        return propertyRootPath.isAbsolute() ? propertyRootPath
                : Paths.get(config.getWorkspacePath(), rootPathString).toAbsolutePath();
    }

    private static ScriptRepository readRepoConfig(@NotNull final Configuration config,
            @NotNull final String propertyPrefix,
            @NotNull final Logger log,
            final boolean globalUpdateEnabled,
            final boolean globalGcEnabled,
            @Nullable final String defaultBranch,
            final boolean resetGitLockFiles,
            @NotNull final String repoName) {
        final Set<String> userNames =
                config.getNameStringSetFromProperty(propertyPrefix + "repo." + repoName + ".groups");
        final boolean updateEnabled =
                globalUpdateEnabled && config.getBoolean(propertyPrefix + "repo." + repoName + ".updateEnabled");
        final boolean gcEnabled = globalGcEnabled
                && config.getBooleanWithDefault(propertyPrefix + "repo." + repoName + ".gcEnabled", true);
        final String remoteOrigin =
                config.getStringWithDefault(propertyPrefix + "repo." + repoName + ".remote", "origin");
        final String branch =
                config.getStringWithDefault(propertyPrefix + "repo." + repoName + ".branch", defaultBranch);
        Require.requirement(!(updateEnabled && branch == null), "!(updateEnabled && branch == null)");
        final boolean prefixDisplayPathsWithRepoName =
                config.getBoolean(propertyPrefix + "repo." + repoName + ".prefixDisplayPathsWithRepoName");
        final Path rootPath =
                normalizeRootPath(config, config.getProperty(propertyPrefix + "repo." + repoName + ".root"));
        final String gitURI = config.getProperty(propertyPrefix + "repo." + repoName + ".uri");
        final String[] searchPathSuffixes =
                config.getProperty(propertyPrefix + "repo." + repoName + ".paths").trim().split("[, ]+");
        final Path[] searchPaths = new Path[searchPathSuffixes.length];
        for (int spi = 0; spi < searchPathSuffixes.length; ++spi) {
            searchPaths[spi] = rootPath.resolve(searchPathSuffixes[spi]);
        }
        log.info().append("Loading Git Repo: ").append(repoName)
                .append(". Branch: ").append(branch != null ? branch : "<none>")
                .append(". Root Path: ").append(rootPath.toString())
                .append(globalUpdateEnabled ? ". Repository updates enabled" : ". Repository updates disabled")
                .append(globalGcEnabled ? ". Git GC enabled." : ". Git GC disabled.").endl();

        return new ScriptRepository(log, repoName, userNames, gitURI, updateEnabled, gcEnabled, remoteOrigin, branch,
                prefixDisplayPathsWithRepoName, rootPath, resetGitLockFiles, searchPaths);
    }

    private static List<ScriptRepository> readRepoConfigs(@NotNull final Configuration config,
            @NotNull final String propertyPrefix,
            @NotNull final Logger log,
            final boolean globalUpdateEnabled,
            final boolean globalGcEnabled,
            @Nullable final String defaultBranch,
            final boolean resetGitLockFiles,
            @NotNull final String... repoNames) {
        final List<ScriptRepository> scriptRepositories = new ArrayList<>(repoNames.length);
        for (final String repoName : repoNames) {
            if (repoName.isEmpty()) {
                continue;
            }
            scriptRepositories.add(readRepoConfig(config, propertyPrefix, log, globalUpdateEnabled, globalGcEnabled,
                    defaultBranch, resetGitLockFiles, repoName));
        }
        return scriptRepositories;
    }

    public static List<ScriptRepository> readRepoConfigs(@NotNull final Configuration config,
            @SuppressWarnings("SameParameterValue") @NotNull final String propertyPrefix,
            @NotNull final Logger log,
            final boolean globalUpdateEnabled,
            final boolean globalGcEnabled,
            @Nullable final String defaultBranch,
            final boolean resetGitLockFiles) {
        return readRepoConfigs(config,
                propertyPrefix,
                log,
                globalUpdateEnabled,
                globalGcEnabled,
                defaultBranch,
                resetGitLockFiles,
                config.getProperty(propertyPrefix + "repos").trim().split("[, ]+"));
    }
}
