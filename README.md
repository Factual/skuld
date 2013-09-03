<img src="https://raw.github.com/Factual/skuld/master/doc/rackham.jpg" style="width: 100%" />

# Skuld

Skuld is (or aims to become) a hybrid AP/CP distributed task queue, targeting
linear scaling with nodes, robustness to N/2-1 failures, extremely high
availability for enqueues, guaranteed at-least-once delivery, approximate
priority+FIFO ordering, and reasonable bounds on task execution mutexing. Each
run of a task can log status updates to Skuld, checkpointing their progress and
allowing users to check how far along their tasks have gone.

Skuld combines techniques from many distributed systems: Dynamo-style
consistent hashing, quorums over vnodes, and anti-entropy provide the
highly-available basis for Skuld's immutable dataset, including enqueues,
updates, and completions. All AP operations are represented by Convergent
Replicated Data Types (CRDTs), ensuring convergence in the absence of strong
consistency. CP operations (e.g.  claims) are supported by a leader
election/quorum protocol similar to Viewstamped Replication or Raft, supported
by additional constraints on handoff transitions between disparate cohorts.

Skuld relies on semi-synchronized clocks: the network, GC pauses, and clock
skew may introduce message skew of no more than eta seconds. Skuld is intended
for systems where eta is on the order of minutes to hours, and task execution
failures may delay retries by at least eta. Failure to maintain this bound
could result in overlapping task execution or other anomalies, though outright
data loss is unlikely.

## Current status

Skuld is experimental at best. Many subsystems are slow, buggy, incorrect, or
missing. We're open-sourcing Skuld at this juncture because many people have
expressed interest in contributing code. *Do not use Skuld in production.* It
*will* explode and destroy your data.

Right now, Skuld can:

- Create and destroy clusters.
- Balance vnodes
- Heal failures via merkle-tree-backed active anti-entropy
- Elect leaders when a majority component is available
- Enqueue tasks
- Claim tasks
- Mark tasks as completed

Major drawbacks include but are not limited to:

- Losing all data on nodes when restarted
- Performing an O(tasks) scan over all tasks for every claim.
- A test suite which deadlocks or fails 95% of the time for unknown reasons
- Merkle trees are O(n log n) in size, including a leaf node for every task
- All sorts of poorly chosen, hardcoded timeouts and parameters
- Task listing holds all tasks in memory
- No concept of streaming requests
- Clock sync detector doesn't do anything when it finds nodes are misaligned
- Incoherent logging
- All cluster transitions rely on Zookeeper (via Apache Helix)
- Probably a whole bunch of bugs I don't know about yet

And missing subsystems include:

- HTTP interface
- Protobuf/TCP interface
- Named queues (right now all tasks are in a single FIFO queue)
- Priorities
- Task updates
- Task dependencies
- Garbage collection

## How can I help?

Hop on #skuld on Freenode, or hit me up at <a
href="mailto:kingsbury@factual.com">kingsbury@factual.com</a>; I'm happy to
answer questions and help you get started working on improvements. You'll want
a basic command of Clojure to start, but distributed systems expertise isn't
required; there are lots of Skuld internals from disk persistence to data structures to HTTP servers that need your help!

## Getting started

You'll need a Zookeeper cluster, and lein 2. To create a cluster, run:

```
lein run cluster create skuld -z some.zk.node:2181 --partitions 8 --replicas 3
```

And add a few nodes. Yeah, this is slow. I'm sorry.

```
lein run cluster add --host "127.0.0.1" --port 13000
lein run cluster add --host "127.0.0.1" --port 13001
lein run cluster add --host "127.0.0.1" --port 13002
```

Open a few shells, and fire em up!

```
lein run start --host 127.0.0.1 --port 13000
lein run start --host 127.0.0.1 --port 13001
lein run start --host 127.0.0.1 --port 13002
```

Open a repl (`lein repl`)

```clj
; Suck in the library
skuld.bin=> (use 'skuld.client)

; And set up the request ID generator
skuld.bin=> (skuld.flake/init!)

; OK, let's define a client with a few hosts to talk to:
skuld.bin=> (def c (client [{:host "127.0.0.1" :port 13000} {:host "127.0.0.1" :port 13001]))

; There are no tasks in the system now:
skuld.bin=> (count-tasks c)
0

; We can enqueue a task with some payload
skuld.bin=> (enqueue! c {:data "sup?"})
#<Bytes 00000140d20fa086800000019dace3c8ab1f6f82>

; Now there's 1 task
skuld.bin=> (count-tasks c)
1

; Which we can show like so:
skuld.bin=> (pprint (list-tasks c))
({:data "sup?",
  :id #<Bytes 00000140d20fa086800000019dace3c8ab1f6f82>,
  :claims []})
nil

; And now we can claim a task for 10 seconds
skuld.bin=> (def t (claim! c 10000))
#'skuld.bin/t
skuld.bin=> (pprint t)
{:data "sup?",
 :id #<Bytes 00000140d20fa086800000019dace3c8ab1f6f82>,
 :claims [{:start 1377913791803, :end 1377913801803, :completed nil}]}
nil

; We can't claim any other tasks during this time
skuld.bin=> (claim! c 10000)
nil

; But if we wait long enough, Skuld will decide the claim has expired. Once a
; quorum of nodes agree that the claim is outdated, we can re-claim the same
; task:
skuld.bin=> (Thread/sleep 60000) (def t2 (claim! c 10000))
nil
#'skuld.bin/t2
skuld.bin=> t2
{:data "sup?", :id #<Bytes 00000140d20fa086800000019dace3c8ab1f6f82>, :claims [{:start 1377913791803, :end 1377913801803, :completed nil} {:start 1377913903904, :end 1377913913904, :completed nil}]}

; Now let's mark that task, in claim 1, as being complete:
skuld.bin=> (complete! c (:id t2) 1)
2

; If we ask Skuld what happened to the task, it'll show both claims, one
; completed:
skuld.bin=> (pprint (get-task c (:id t2)))
{:data "sup?",
 :id #<Bytes 00000140d20fa086800000019dace3c8ab1f6f82>,
 :claims
 [{:start 1377913791803, :end 1377913801803, :completed nil}
  {:start 1377913903904, :end 1377913913904, :completed 1377913942979}]}
nil
```

## Concepts

Skuld works with queues, which are named by strings. Queues comprise a set of
tasks. Tasks are identified by a unique, k-ordered string id. A task has some
immutable payload, which is arbitrary bytes; a priority, which is a signed
32-bit integer (higher numbers dequeued first), and a list of dependencies,
which are the keys of other tasks. A task is only eligible for claim when all
its dependencies are complete. Tasks can be claimed by a worker at least once,
and Skuld makes every effort to guarantee claims do not temporally or causally
overlap; providing monotonic claim sequence identifiers to clients to assist in
this. Claims last until some time t; workers must regularly renew their claim
on the task if that time approaches, or risk the claim expiring.

As work occurs on a task, Skuld can add immutable updates for that task, which
form a task log of the task's progress. Tasks can be marked as complete when
they are finished and should not be run again. Updates and the complete message
include a mandatory sequence number, which provides a total order for the
history of the task and ensures only contiguous histories are visible. At any
point, one can ask about the current state of a task, receiving the task
payload, claim state, priority, etc; optionally including the sequence number
of the most recent update seen, to ensure monotonicity.

If a claim expires and the task is not flagged as complete, it is eligible for
claim again, and can be retried under a new claim ID. Workers can request the
log for a task's earlier claims–for example, to recover from a checkpoint.

When every task connected by dependency pointers is complete, those tasks may
be eligible for garbage collection, flushed to long-term storage, or retained
indefinitely.

## Operations

### Topology

The ID space is hashed and sharded into partitions. Partitions are claimed by
cohorts of N nodes. One node is elected the leader for that partition, and all
others are followers. The leader is responsible for handing out claims and
recovering failed tasks. The followers receive claim messages from the leader,
so that all N nodes maintain a somewhat-in-sync set of the currently claimed
tasks. All other messages (enqueue, update, complete, etc) are eventually
consistent, and are gossiped continuously between all N nodes via hash trees.

### Enqueue

To enqueue a task, generate a 160-bit flake ID and assign it to the task.
Compute the partition for that ID. Broadcast the task to all nodes in that
partition. Wait for a quorum of nodes to respond by default; though the client
could request a smaller or larger W val. Once a W nodes have acked the task,
respond to the client with the number of acking nodes. By giving the generated ID back to the client, the client can retry its write idempotently.

### Claim

Try to find a leader on the local node which has a task to claim. If there are
none, proxy the claim request to each other node in a randomized order, and
look for local leaders there. The leader selects a task which is a.) in the
correct queue, b.) unclaimed, c.) uncompleted, d.) has the highest priority,
e.) is the oldest. The leader examines the claim set for that task, and selects
a new claim ID. Primary broadcasts a message to all nodes in cohort requesting
that they mark that task as claimed until time t. Once a quorum have
acknowledged that operation, the leader considers the task claimed and returns
the task, and claim ID, to the coordinator.

### Renew

Claims are only valid for a certain amount of time. The client must regularly
renew its claim to prevent it from expiring. To renew a claim, the node
broadcasts the renew message to all peers for that partition. If a quorum
respond, the renew is successful. Renew is idempotent.

### Update

The client must maintain an internal sequence number, beginning at zero, and
incrementing by one for each update. To send an update for a task, the
client sends its update, with the claim number and sequence number, to any
node. That node broadcasts the update to all nodes for that task's partition.
When W nodes have acknowledged the update, the coordinator returns success to
the client. The client should retry updates which fail for as long as
reasonably possible. Update is idempotent.

### Complete

To mark the message as complete, send a complete message to any node with a sequence number, just like an update. The coordinator broadcasts that update to all nodes for the task's partition. Those nodes immediately remove the task from their claim set, write the complete message to disk, and return an acknowledgement. When W nodes have acknowledged the update, the coordinator returns success to the client.

### Clocksync

All nodes regularly exchange heartbeat messages with their current clock. If
any differ by more than a few seconds, the furthest ahead kills itself.

### Active anti-entropy

All nodes regularly gossip Merkle trees of their immutable dataset to any peers in their partition, and replicate missing blocks. All followers regularly exchange Merkle trees of their claim set with the current leader, and copy the leader's data where different.

### Leaders

In becoming a leader, we need to ensure that:

1. Leaders are logically sequential
2. Each leader's claim set is a superset of the previous leader

We have: a target cohort of nodes for the new epoch, provided by Helix.
Some previous cohort of nodes belonging to the old epoch, tracked by ZK.

To become a leader, one must successfully:

1. Read the previous epoch+cohort from ZK

2. (optimization) Ensure that the previous epoch is strictly less than the
epoch this node is going to be the leader for.

3. Broadcast a claim message to the new cohort, union the old cohort

4. Receive votes from a majority of the nodes in the old cohort

5. Receive votes from a majority of the nodes in the new cohort

  - At this juncture, neither the new nor the old cohort can commit claims
for our target epoch or lower, making the set of claims made in older epochs
immutable. If we are beat by another node using a newer epoch, it will have
recovered a superset of those claims; we'll fail to CAS in step 8, and no
claims will be lost.

6. Obtain all claims from a majority of the old cohort, and union them in to
our local claim set.

  - This ensures that we are aware of all claims made prior to our epoch.

7. Broadcast our local claim set to a majority of the new cohort.

  - This ensures that any *future* epoch will correctly recover our claim
    set. 6 + 7 provide a continuous history of claims, by induction.

8. CAS our new epoch and cohort into zookeeper, ensuring that nobody else
   beat us to it.

If any stage fails, delay randomly and retry.

A note on zombies:

Zombies are nodes which are ready to give up ownership of a vnode but cannot,
because they may still be required to hand off their claim set. After step 8,
we inform all zombies which are not a part of our new cohort that it is safe
to drop their claim set."


### Recovery

A vnode allows a task to be claimed, in the event of failure, once the claim's
time has expired relative to the local clock *including* a buffer larger than
the maximum clock skew, message propagation delay, GC time, etc: on the order
of minutes to hours. Shorter buffers increase the probability that claims will
overlap.

## License

Copyright © 2013 Factual, Inc

Distributed under the Eclipse Public License, the same as Clojure.
