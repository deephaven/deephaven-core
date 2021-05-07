## Git subtree

Git subtrees are an alternative to git submodules.
While git submodules use commit links as first class citizens, git subtrees stick with commits as the mode of sharing.
Git subtrees add commits from a remote repository into a local repository.
Unless you are adding, or updating, a subtree, the developer and CI workflow is unimpeded by subtrees.

### `py/jpy` subtree

By using a subtree for jpy, we are able to get rid of the explicit `lib/jpy/jpy.jar` dependency.
We are preferring to use the `--squash` flag to cut down on the amount of jpy history that we need to pull in.
Furthermore, no changes (save for explicit subtree pulls) should be made in the `py/jpy` subtree.
Any development work should be made in the appropriate jpy repo.

#### subtree add
```bash
cd iris/

# This "adds" illumon-jpy to the *local* repository as a remote, so we can bring the commits into the iris repository.
# No references to this remote-name / url make it past the local machine (even if we push, and don't remove it).
git remote add --fetch illumon-jpy https://github.com/illumon-public/illumon-jpy.git

# This adds the tree referenced by illumon-jpy/master into the iris repository, and merges it with the current HEAD.
# Since we used --squash, the tree is squashed into a single commit.
# The subtree's root will not have a parent commit.
git subtree add --prefix py/jpy illumon-jpy master --squash

# Optional, but suggested. Removes the remote, because we don't need any more references to it until we update jpy.
git remote remove illumon-jpy

# Development proceeds as normal
```

Example history:
```
*   8a03d2b17 (HEAD -> jpy_subtree) Merge commit '61d12eea4d7e784e4b93adef623a7f70140ebd8f' as 'py/jpy'
|\
| * 61d12eea4 Squashed 'py/jpy/' content from commit 370b0d8cc
* bc2913b1c (develop) Fixed Bug Where Descriptions Would Break If you had A TreeTable
* ec14eecf6 IDS-3121: Reapply Rollup
...
```

#### subtree pull
```bash
cd iris/

# Required if illumon-jpy is not already a remote.
git remote add --fetch illumon-jpy https://github.com/illumon-public/illumon-jpy.git

# This pulls the tree referenced by illumon-jpy/master into the iris repository, and merges it with the current HEAD.
# Since we used --squash, the changes are squashed into a single commit.
# The subtree's new commit will reference use the previous subtree's commit as its parent.
git subtree pull --prefix py/jpy illumon-jpy master --squash

# Optional, but suggested. Removes the remote, because we don't need any more references to it until we update jpy.
git remote remove illumon-jpy

# Development proceeds as normal
```

Example history:
```
*   a410b1f4e (HEAD -> jpy_subtree) Merge commit 'e51bda4cd6f8132118d79eb2b591736b6c4e8602' into jpy_subtree
|\
| * e51bda4cd Squashed 'py/jpy/' changes from 370b0d8cc..ae813df53
* | 9b3162952 fakework (other developers continue developing...)
* | afb0ffe77 add deephaven-jpy as a project
* |   8a03d2b17 Merge commit '61d12eea4d7e784e4b93adef623a7f70140ebd8f' as 'py/jpy'
|\ \
| |/
| * 61d12eea4 Squashed 'py/jpy/' content from commit 370b0d8cc
* bc2913b1c Fixed Bug Where Descriptions Would Break If you had A TreeTable
* ec14eecf6 IDS-3121: Reapply Rollup
...
```

#### subtree push
```bash
# We *shouldn't* be using subtree push.
# This is only necessary if you are doing development work directly in the subtree.
# In our case, all subtree work should instead be done in the appropriate main repo, and then pulled in as appropriate.
git subtree push ...
```

### References
* [Atlassian example](https://www.atlassian.com/blog/git/alternatives-to-git-submodule-git-subtree)
* [git-scm book](https://git-scm.com/book/en/v1/Git-Tools-Subtree-Merging)
