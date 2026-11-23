# Barrage viewport requests and subscription growth

How `BarrageMessageProducer` serves a viewport request as soon as possible, how a full subscription
grows to the whole table over several snapshots without stalling the update graph, how one snapshot
round's row budget is shared between the two, and the order in which a propagation phase writes the
phase's subscribers.

Code: `server/src/main/java/io/deephaven/server/barrage/BarrageMessageProducer.java`, mainly
`updateSubscription`, `updateSubscriptionsSnapshotAndPropagate`, `reserveSnapshotPositions`,
`finalizeSnapshotForSubscriptions` and `forEachInParallel`.

## The problem

A subscription that is new, or whose viewport, columns or direction changed, needs rows the producer
has not yet sent the subscription. The producer sends those rows in snapshots. `ConstructSnapshot`
first takes a snapshot concurrently with the update graph, without a lock, and discards the attempt
when the table ticks during the attempt. After `ConstructSnapshot.maxConcurrentAttempts` failed
attempts (2 by default), or after one failed attempt longer than
`ConstructSnapshot.maxConcurrentAttemptDurationMillis` (5 s by default), the snapshot takes the
update graph's shared lock. No update cycle runs until the locked snapshot finishes, so every table
on the update graph stops ticking, not only the subscribed table. A large snapshot of a ticking
table is the most likely to fail concurrently and holds the lock the longest, so the producer caps
the size of each snapshot.

The two kinds of subscription pull against that cap in opposite directions:

- **Viewport requests must be served as soon as possible.** A viewport usually backs a UI. When a
  user scrolls, the UI asks for a new viewport, and the user waits for the rows. A viewport filled
  over several snapshots shows the user a partial view, and a full subscription's work must never
  delay a viewport request.
- **Full subscriptions can wait, and grow.** A full subscription that needs more rows than one
  snapshot allows **grows** over several snapshots, behind the viewport requests. Growth has its own
  hazards:
  - **Oversized snapshots.** Every row a round snapshots adds to the snapshot's length, and to the
    chance the snapshot falls back to the update graph's lock. A round's full-subscription rows must
    stay within the budget, whatever the table does between planning the round and taking the
    snapshot.
  - **Tables that grow faster than the budget.** A table can gain more rows per round than a round
    may snapshot. A full subscription whose completion depends on catching up to the end of the
    table then never completes, and keeps costing a snapshot every round. Completion must not depend
    on the table's growth rate.
  - **Several full subscriptions growing at once.** Growing full subscriptions share what budget the
    viewport requests leave. Positions that hold no rows must not use up the budget, a row reserved
    once must serve every subscription that needs the row, and a full subscription that started
    early must not wait for a full subscription that started later.
  - **Wide subscriptions.** The budget is set in cells, and a subscription to many columns gets few
    rows per round. Rounding the row count down to zero would snapshot nothing, and a full
    subscription that gains no rows never completes.

## Terms

- **Position:** a row's index in the table's current order, 0 to size − 1. A reverse viewport counts
  positions from the end of the table, so position 0 is the last row.
- **Viewport:** a set of positions, never row keys. Every row set the producer keeps to track what a
  subscription has or needs is in position space: a viewport subscription's viewport, a growing full
  subscription's viewport, and the remaining positions.
- **Viewport subscription:** a subscription to a viewport, usually backing a UI.
- **Full subscription:** a subscription to every row of the table.
- **Viewport request:** a new viewport subscription, or a change of an existing viewport
  subscription's viewport, columns or direction. A viewport request is served whole in the next
  snapshot round, ahead of every full subscription, and is never split over several rounds.
- **Growing full subscription:** a full subscription, new or with changed columns, that does not yet
  hold every row. Only full subscriptions grow over several rounds.
- **Stable subscription:** a subscription that holds every row the subscription asked for. A stable
  subscription receives only deltas and takes no snapshot budget.
- **Remaining positions** (`growingRemainingViewport`): the positions a viewport request or a
  growing full subscription has not yet received. A subscription with remaining positions is marked
  `isGrowingViewport`, despite the name: a viewport request until the request's snapshot, a growing
  full subscription until the subscription holds the whole table. The field's Javadoc lists what the
  flag drives.
- **Snapshot round:** one pass of `updateSubscriptionsSnapshotAndPropagate` that plans a snapshot for
  the pending viewport requests and growing full subscriptions, takes the snapshot, and writes the
  snapshot to those subscriptions.
- **Row budget** `B`: the number of rows a round's snapshot aims for,
  `max(1, clamp(snapshotTargetCellCount, minSnapshotCellCount, maxSnapshotCellCount) / C)`, where
  `C` is the number of columns the round's subscriptions ask for, at least 1. The producer tunes
  `snapshotTargetCellCount` after each snapshot that includes rows, toward
  `BarrageUtil.targetSnapshotPercentage` of the update interval.
- **`T`:** the table size when a round is planned, read from `parentTableSize` under the producer's
  lock.

## Design

### Scheduling a viewport request

`updateSubscription` records the request as the subscription's pending change and schedules the
producer's propagation job immediately, without waiting for the update interval. The job's next
snapshot round serves the request. The producer runs one propagation job at a time, so a request
that arrives while a job is running waits for that job to finish, and the request is served in the
round right after.

### One snapshot round

1. **Order.** The round takes the viewport requests first, then the growing full subscriptions
   sorted by remaining positions, fewest first. Full subscriptions in one direction grow from
   position 0 upward, so a full subscription that started later still needs every position an
   earlier full subscription needs, and the rows reserved for the full subscription nearest
   completion serve every full subscription in that direction. Sorting each round makes the order
   independent of the order the producer holds the subscriptions in, which changes when a
   subscription is removed.
2. **Viewport requests.** Each viewport request first takes the positions earlier requests already
   reserved this round, then reserves every remaining position of the requested viewport, however
   large the viewport and however little budget is left: a viewport request is never cut short to
   fit the budget. The positions below `T` hold rows and count against `B`, which may go below zero,
   leaving nothing for full subscriptions. The positions at or past `T` hold no rows, so the
   snapshot includes nothing for those positions and the positions cost nothing. The request's
   remaining set is now empty. When the viewport requests exceed `B`, the producer logs the overrun
   at debug level.
3. **Growing full subscriptions.** While budget remains, each growing full subscription takes the
   positions already reserved, then reserves remaining positions below `T`, up to the budget left. A
   full subscription never reserves a position at or past `T`.
4. **Catch-up pass.** Every subscription in the round takes the remaining positions that
   subscriptions later in the round reserved, so a position reserved once serves every subscription
   that needs the position in that direction.
5. **Snapshot.** The producer snapshots the reserved positions of the subscriptions' columns.
6. **Finalize.** Under the producer's lock, where `parentTableSize` matches the snapshot,
   `finalizeSnapshotForSubscriptions` moves each growing full subscription's remaining positions at
   or past `parentTableSize` into the subscription's viewport. The snapshot held no rows there, and
   once `promoteSnapshotToActive` scopes the deltas to the new viewport, every row later added at
   those positions reaches the subscription as an ordinary delta. Every viewport request, and every
   full subscription whose remaining set is now empty, becomes stable: the subscription's viewport
   becomes the target viewport, `null` for a full subscription, which is how a client detects
   completion. Blink tables are excluded from the move, and a full subscription to a blink table is
   stable after the first snapshot, because a blink table's snapshot is empty.
7. **Write.** The producer writes the snapshot to each subscription, viewport subscriptions first
   (see [Write order](#write-order)).

Rows the table gains between planning (step 1, `T`) and the snapshot (step 5) sit at positions from
`T` up to the snapshot's table size. No full subscription reserved those positions, so the
full-subscription rows stay within `B` however fast the table grows, and those positions stay in
the remaining set until a later round, which plans with a larger `T`.

### Position space for a growing full subscription

A growing full subscription has a viewport too, a set of positions like any other viewport, until
the subscription completes and the viewport becomes `null`. The viewport and the remaining positions
together always cover every position from 0 to `Long.MAX_VALUE - 1`. A round moves positions from
the remaining set into the viewport, and nothing moves positions back. So the remaining set only
shrinks, and the subscription completes when the remaining set is empty.

Rows move between positions as the table changes. A row inserted in the middle of the table shifts
every later row up by one position, which can move a row the subscription holds into a remaining
position, or a row the subscription lacks into the viewport. The deltas scoped to the viewport carry
those moves, and a later round snapshots whatever row then sits at a remaining position. When the
remaining set is empty the viewport covers every position, so the subscription holds every row of
the table.

### Write order

Each propagation phase, snapshot phases and delta phases alike, writes every viewport subscriber
before any full subscriber. `forEachInParallel` splits the phase's targets with
`isFullSubscription`, which counts a subscription as full when the subscription has no viewport, has
a pending change to a full subscription, or is a growing full subscription. The producer then runs
two `invokeParallel` calls on the propagation scheduler: the viewport targets', then the full
targets'. Groups of identical stable full subscribers (see `shared-subscriber-writes.md`) are full
targets, and the largest group still starts first among the full targets.

A viewport subscriber's write is small and usually feeds a UI that someone is watching, while a full
subscriber's write grows with the table, so a phase's full writes never delay the phase's viewport
updates. Each write handles the write's own subscribers' failures. A failure that no viewport write
handles ends the phase before any full write starts.

## Guarantees

**A viewport request is served in one round.** The round that picks up a viewport request reserves
every remaining position of the viewport, whatever the viewport's size and whatever the growing full
subscriptions need, so the viewport arrives in one snapshot, written before any full subscriber.

**Only viewport requests exceed the budget.** A round's snapshot holds the viewport requests' rows
plus at most `L = max(0, B − V)` more rows for growing full subscriptions, where `V` is the in-table
positions the viewport requests reserved. That bound holds however much the table grows between
planning and the snapshot.

**Every round makes progress.** `B` is at least 1 row, so a round with any growing full subscription
snapshots rows and the cell budget keeps being tuned. The snapshot itself falls back to a locked
snapshot after `ConstructSnapshot`'s concurrent attempts run out, so every round ends.

**A full subscription completes in bounded rounds, whatever the growth rate.** The bound below holds
while every growing full subscription grows in the same direction and the viewport requests leave a
budget `L > 0`. After a full subscription's first snapshot, the subscription's remaining set holds
only positions below the table size at that snapshot, and the set only shrinks. Each round reserves
up to `L` positions that the full subscription nearest completion still needs, and every full
subscription in that direction that needs a reserved position receives the position that round.
Every later round plans with a table size at least the first snapshot's, so every remaining position
can be reserved. A full subscription with `R` positions left after the first snapshot therefore
completes within `⌈R / L⌉` more rounds, `⌈R / L⌉ + 1` counting the round of the first snapshot,
where `L` is the smallest budget the viewport requests leave over those rounds.

**A full subscription never waits for a later full subscription in the same direction.** A full
subscription that started later in the same direction needs every position the earlier subscription
needs and sorts after the earlier subscription, so the earlier subscription completes on the earlier
subscription's own remaining positions.

**Viewport writes come first.** No full subscriber's write in a phase starts before every viewport
subscriber's write in the phase has finished.

**What is not guaranteed:**

- A viewport request that arrives while a propagation job is running waits for that job, including
  the job's full-subscriber writes, before the next round serves the request.
- Growing full subscriptions gain no rows in a round where the viewport requests take the whole
  budget. Viewport requests in every round stall full growth for as long as the requests continue.
- Forward and reverse full subscriptions do not share reservations. A reservation serves only the
  subscriptions in the reserving subscription's direction, and the budget goes to the subscription
  nearest completion in either direction. A full subscription can therefore wait for
  opposite-direction full subscriptions nearer completion, including ones that started later, and
  the bound above does not apply.

## Trade-offs

- **Viewport requests exceed the budget.** A client that asks for a very large viewport gets the
  whole viewport in one snapshot, larger and longer than the budget aims for. On a ticking table,
  that snapshot is more likely to fall back to the update graph's lock, and stops every table on the
  update graph from ticking for longer.
- **Snapshot messages.** A growing full subscription carries a viewport ending at
  `Long.MAX_VALUE - 1`. Clients must treat the viewport as a set of positions, never as a count, and
  detect completion as a `null` viewport; the Java, JS and C++ clients do.
- **Delta recording.** While a full subscription grows, the deltas record the data of appended rows
  for the subscription. The work is the same one stable full subscription causes, adds nothing when
  a stable full subscription already exists, and is less than snapshotting those rows later.
- **Full writes wait for viewport writes.** A full subscriber's update is delayed by the time the
  phase's viewport writes take.

Clients receive a viewport request's rows as one snapshot message, and receive rows appended while a
full subscription grows as updates, with the rows' data.

## Decisions recorded

| Decision | Why |
| --- | --- |
| Schedule the propagation job immediately for a viewport request | A user is waiting on the rows; the update interval paces deltas, not requests |
| Serve a viewport request whole in one round, past the budget | A viewport usually backs a UI; filling the viewport over several rounds shows the user a partial view |
| Charge viewport rows that exist, not past-end positions | Past-end positions cost nothing to snapshot, and charging those positions would starve full subscriptions for no work |
| Full subscriptions reserve only positions below `T` | Keeps a round's full-subscription rows within the budget however much the table grows before the snapshot |
| Past-end positions join a growing full subscription's viewport at the snapshot | Turns rows appended later into deltas, so the remaining set only shrinks and completion does not depend on the table's growth |
| Sort growing full subscriptions by remaining positions every round | The nearest-complete subscription's reservation serves every full subscription, whatever order the producer holds the subscriptions in |
| A row budget of at least 1 | A zero budget snapshots nothing, so the cell budget, tuned only after a snapshot with rows, never recovers |
| Viewport writes before full writes, in every phase | Small, latency-sensitive writes go first; one split in `forEachInParallel` covers per-subscriber writes, groups and snapshot writes alike |

## Open questions

- **A viewport request behind a running job.** A request that arrives while a round is snapshotting
  or writing for full subscriptions waits for that round to finish. Serving requests in a job of
  their own would need the two jobs to agree on the producer's snapshot and delta state. Not
  planned.
- **Viewport requests in every round.** Requests in every round can leave growing full
  subscriptions no budget. A floor for full subscriptions, or a cap on how far viewport requests may
  exceed the budget, would bound full growth at the cost of viewport latency. Not planned.
- **Snapshot size for huge viewports.** A viewport has no size limit. A client that asks for a
  viewport covering most of a large table gets a snapshot of that size in one round.

## Tests

In `server/src/test/java/io/deephaven/server/barrage/`. Each test that waits for growth bounds the
number of rounds with a snapshot hook that fails the test, so a regression fails instead of hanging.
The tests size the tables from `BarrageUtil.MIN_SNAPSHOT_CELL_COUNT`, the smallest budget any round
can have, so the tests span several rounds under any snapshot budget.

| Guarantee | Test |
| --- | --- |
| A viewport request is served in one round | `BarrageMessageSubscriptionRoundTripTestBase.testViewportSubscriptionSatisfiedInOneRound` |
| Full subscriptions get only what viewport requests leave | `testViewportLargerThanBudgetLeavesFullSubscriptionsNothing`, `testFullSubscriptionsGetTheBudgetViewportsLeave` |
| A late full subscription shares the budget | `testLateFullSubscriptionSharesGrowthBudget` |
| The full-subscription rows stay within the budget as the table grows | `testGrowthRoundStaysWithinBudgetWhenTableGrows` |
| The row budget is never zero | `testSubscriptionWiderThanCellBudgetGrows` |
| Completion whatever the growth rate | `testFullSubscriptionCompletesWhenGrowthExceedsBudget` |
| Nearest completion grows first, after a removal | `testNearestCompleteFullSubscriptionGrowsFirst` |
| No waiting for a later full subscription | `testFullSubscriptionCompletesWithoutWaitingForLaterOnes` |
| A reverse full subscription completes as appended, inserted and removed rows cross the boundary | `testReverseFullSubscriptionCompletesAsRowsCrossTheBoundary` |
| Viewport writes come first | `BarrageMessageParallelPropagationTest.testViewportSubscribersAreWrittenFirst` |

The C++ client's `growing_subscription_test.cc` checks that rows appended while a full
subscription's snapshot is taken arrive in one update with the rows' data.
