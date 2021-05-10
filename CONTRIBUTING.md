# Contributing

This guide will serve as a reference for contributing to the Deephaven.

## Getting the source

We use forks to keep the branches in [deephaven/deephaven-core](https://github.com/deephaven/deephaven-core) repository clean. [Create your own fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo#fork-an-example-repository) by clicking the Fork button in the top right. You should select to fork it under your own username. Then, clone your fork locally:
```
git clone git@github.com:<username>/deephaven-core.git
```
You can then create your own branches and push them to your forked repository.

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
