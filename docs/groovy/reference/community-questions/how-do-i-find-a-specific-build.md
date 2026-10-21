---
title: How can I find which Core version added a feature or fix?
---

At the bottom of a Deephaven Community Core commit message you can see branches and tags containing that particular change. You can browse commits in the [deephaven-core GitHub repository](https://github.com/deephaven/deephaven-core/commits/main/). Click on any title for details. For example, in the following commit message, you can see that 0.31.0 is the only release that includes this merge commit:

![A commit message with branches and tags highlighted](../../assets/reference/community-questions/commitmessage.png)

If no release tag is listed, the change isn't in a release yet.

The deephaven-core repository holds only Core changes. Web UI changes are tagged in the [deephaven/web-client-ui](https://github.com/deephaven/web-client-ui) repository.
