# Issue Triaging

Loosely based on the [VSCode triage process](https://github.com/microsoft/vscode/wiki/Issues-Triaging) (not as automated - yet).

1. File an issue using the [Bug Report](https://github.com/deephaven/deephaven-core/issues/new?assignees=&labels=bug%2C+triage&template=bug_report.md) or the [Feature Request](https://github.com/deephaven/deephaven-core/issues/new?assignees=&labels=feature+request%2C+triage&template=feature_request.md) template. The issue will be created with the `Triage` label.
2. The issue triager (weekly rotating role) goes through the unassigned [`triage` issues](https://github.com/deephaven/deephaven-core/labels/triage), assigning to the appropriate feature area owner. By assigning it directly to someone, they will see the issue on their issue list, and can see it needs triaging because of the `triage` label. If you are unsure who to assign it to, take your best guess and the owner can re-assign if it is not for them:
     - [web ui](https://github.com/deephaven/deephaven-core/issues?q=is%3Aopen+is%3Aissue+no%3Amilestone+label%3A%22web+ui%22) - Bender
     - [core](https://github.com/deephaven/deephaven-core/issues?q=is%3Aopen+is%3Aissue+no%3Amilestone+label%3Acore+) - Ryan
     - [build](https://github.com/deephaven/deephaven-core/issues?q=is%3Aopen+is%3Aissue+no%3Amilestone+label%3Abuild+) - Devin
     - [grpc](https://github.com/deephaven/deephaven-core/issues?q=is%3Aopen+is%3Aissue+no%3Amilestone+label%3Agrpc+) - Nate
     - [community](https://github.com/deephaven/deephaven-core/issues?q=is%3Aopen+is%3Aissue+no%3Amilestone+label%3Acommunity) - Chip
3. Area owners [triage tickets assigned to them](https://github.com/deephaven/deephaven-core/issues?q=is%3Aopen+label%3Atriage+assignee%3A%40me+) by adding appropriate labels, any additional comments, and adding to the backlog if it is an accepted issue.
   - Tickets may have additional labels added, dependencies linked, and/or comments added.
   - If more information is required, add the [`needs more info` label](https://github.com/deephaven/deephaven-core/labels/needs%20more%20info), and re-assign to the creator.
   - If it's a duplicate of an existing issue, add the [`duplicate` label](https://github.com/deephaven/deephaven-core/labels/duplicate), and close the issue with a comment linking to the issue it duplicates.
   - If the issue is important, such as a blocking issue or a security vulnerability, add the [`important`](https://github.com/deephaven/deephaven-core/labels/important) label, and assign to the current milestone as necessary.
   - After the issue has been validated, add an appropriate area label, remove the `triage` label, unassign yourself as the owner, and add it to the [Backlog milestone](https://github.com/deephaven/deephaven-core/milestone/11). It will be assigned to a milestone when doing monthly planning.
4. Developers work on tickets assigned to them within the current milestone. Since only tickets that you are working on will be assigned to you, [your issue list](https://github.com/deephaven/deephaven-core/issues/assigned/@me) should stay trim/easy to manage.
5. Anybody (within the company or community) can vote on issues in the backlog using reactions. The issues with the most upvotes will be prioritized higher in the [backlog issue list](https://github.com/deephaven/deephaven-core/issues?q=is%3Aopen+milestone%3ABacklog+sort%3Areactions-%2B1-desc).

# Monthly Planning/Backlog Grooming

Each feature area owner (with support from their team) will do backlog grooming by looking at the [backlog issue list](https://github.com/deephaven/deephaven-core/issues?q=is%3Aopen+milestone%3ABacklog+sort%3Areactions-%2B1-desc), breaking down issues into concise chunks with a clear definition of done, and assigning what they think they can get done in the next month. Be aware of dependencies, and work with the product manager in addition to the upvotes for prioritization. For example, the product manager may add a label for a new initiative to focus on for the next month and tag issues in the backlog with those labels, then area owner knows to focus on those issues.
