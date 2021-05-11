# Contributing

This guide will serve as a reference for contributing to the Deephaven.

## Getting the source

Deephaven uses the [Forking Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/forking-workflow).  In this workflow, the [deephaven/deephaven-core](https://github.com/deephaven/deephaven-core) repository contains a minimum number of branches, and development work happens in user-forked repositories.

To learn more see:
* [Forking Workflow](https://www.atlassian.com/git/tutorials/comparing-workflows/forking-workflow)
* [Forking Projects](https://guides.github.com/activities/forking/)
* [Fork A Repo](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo)
* [Working With Forks](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/working-with-forks)

To get started quickly:
1) Navigate to [https://github.com/deephaven/deephaven-core](https://github.com/deephaven/deephaven-core).
2) Click `Fork` in the top right corner.
3) `git clone git@github.com:<username>/deephaven-core.git`
4) Commit changes to your own branches in your forked repository.

Over time, forks will get out of sync with the upstream repository.  Follow these directions on [Syncing A Fork](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/syncing-a-fork) to stay up to date.

## Creating a Pull Request
You can [create a pull request](https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request-from-a-fork) from your pushed fork/branch using the GitHub UI. Alternative, you can use the [GitHub CLI](https://cli.github.com/manual/gh_pr_create) to create a PR from the command line. From within your own fork/branch on the command line, enter:
```
gh pr create -f -w
```
When prompted where the base repository should be, select `deephaven/deephaven-core`.
When prompted where to push the branch, push to `<username>/deephaven-core`.
Your changes should automatically get pushed, and then a new pull request with your changes should open up in your browser. Complete the information in the pull request and click `Create pull request`.

## Styleguide
The [styleguide](style/README.md) is not global yet.
To opt-in, module build files apply the following:

```groovy
spotless {
  java {
    eclipse().configFile("${rootDir}/style/eclipse-java-google-style.xml")
  }
}
```
