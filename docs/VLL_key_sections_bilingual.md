# VLL 重要章节中英对照

说明：
- 本文档从 [vldbj-vll.pdf](/home/idle/Desktop/毕业设计/文献/vldbj-vll.pdf) 中选取了最关键、最适合精读的几个部分，整理为“英文原文 + 中文译文”的对照形式。
- 英文原文基于 PDF 提取结果做了轻微清理，只修正了换行、断词和版面噪声，不改动原意。
- 当前收录的部分包括：`Abstract`、`1 Introduction`、`2.1 The VLL algorithm`、`2.4 Impediments to acquiring all locks at once`、`2.5 Trade-offs of VLL`、`2.6 Selective contention analysis (SCA)`、`5 Conclusion and future work`。

## Abstract

### Original

Lock managers are increasingly becoming a bottleneck in database systems that use pessimistic concurrency control. In this paper, we introduce very lightweight locking (VLL), an alternative approach to pessimistic concurrency control for main memory database systems, which avoids almost all overhead associated with traditional lock manager operations. We also propose a protocol called selective contention analysis (SCA), which enables systems implementing VLL to achieve high transactional throughput under high-contention workloads. We implement these protocols both in a traditional single-machine multi-core database server setting and in a distributed database where data are partitioned across many commodity machines in a shared-nothing cluster. Furthermore, we show how VLL and SCA can be extended to enable range locking. Our experiments show that VLL dramatically reduces locking overhead and thereby increases transactional throughput in both settings.

### 中文译文

在采用悲观并发控制的数据库系统中，锁管理器正越来越成为性能瓶颈。本文提出了超轻量锁机制 `VLL`，它是一种面向主内存数据库系统的替代性悲观并发控制方法，几乎规避了传统锁管理器操作所带来的全部开销。我们还提出了一种名为选择性竞争分析的协议 `SCA`，使实现 `VLL` 的系统在高冲突工作负载下仍能获得较高的事务吞吐量。我们分别在两类环境中实现了这些协议：一类是传统的单机多核数据库服务器，另一类是数据分布在多台通用机器、采用 `shared-nothing` 集群的分布式数据库。进一步地，我们展示了如何把 `VLL` 和 `SCA` 扩展到范围锁。实验结果表明，`VLL` 能显著降低加锁开销，并因此在这两种环境下都提升事务吞吐量。

## 1 Introduction

### Original

As the price of main memory continues to drop, increasingly many transaction processing applications keep the bulk (or even all) of their active datasets in main memory at all times. This has greatly improved performance of OLTP database systems, since disk IO is eliminated as a bottleneck.

As a rule, when one bottleneck is removed, others appear. In the case of main memory database systems, one common bottleneck is the lock manager, especially under workloads with high contention. One study reported that 16-25% of transaction time is spent interacting with the lock manager in a main memory DBMS [12]. However, these experiments were run on a single core machine with no physical contention for lock data structures. Other studies show even larger amounts of lock manager overhead when there are transactions running on multiple cores competing for access to the lock manager [14,22,29]. As the number of cores per machine continues to grow, lock managers will become even more of a performance bottleneck.

Although locking protocols are not implemented in a uniform way across all database systems, the most common way to implement a lock manager is as a hash table that maps each lockable record's primary key to a linked list of lock requests for that record [2,4,5,11,34]. This list is typically preceded by a lock head that tracks the current lock state for that item. For thread safety, the lock head generally stores a mutex object, which is acquired before lock requests and releases to ensure that adding or removing elements from the linked list always occurs within a critical section. Every lock release also invokes a traversal of the linked list for the purpose of determining what lock request should inherit the lock next.

These hash table lookups, latch acquisitions, and linked list operations are main memory operations and would therefore be a negligible component of the cost of executing any transaction that accesses data on disk. In main memory database systems, however, these operations are not negligible. The additional memory accesses, cache misses, CPU cycles, and critical sections invoked by lock manager operations can approach or exceed the costs of executing the actual transaction logic. Furthermore, as the increase in cores and processors per server leads to an increase in concurrency (and therefore lock contention), the size of the linked list of transaction requests per lock increases, along with the associated cost to traverse this list upon each lock release.

We argue that it is therefore necessary to revisit the design of the lock manager in modern main memory database systems. In this paper, we explore two major changes to the lock manager. First, we move all lock information away from a central locking data structure, instead co-locating lock information with the raw data being locked (as suggested in the past [8]). For example, a tuple in a main memory database is supplemented with additional (hidden) attributes that contain information about the row-level lock information about that tuple. Therefore, a single memory access retrieves both the data and lock information in a single cache line, potentially removing additional cache misses.

Second, we remove all information about which transactions have outstanding requests for particular locks from the lock data structures. Therefore, instead of a linked list of requests per lock, we use a simple semaphore containing the number of outstanding requests for that lock (alternatively, two semaphores, one for read requests and one for write requests). After removing the bulk of the lock manager's main data structure, it is no longer trivial to determine which transaction should inherit a lock upon its release by a previous owner. One key contribution of our work is therefore a solution to this problem. Our basic technique is to force all locks to be requested by a transaction at once, and order the transactions by the order in which they request their locks. We use this global transaction order to figure out which transaction should be unblocked and allowed to run as a consequence of the most recent lock release.

The combination of these two techniques, which we call very lightweight locking (VLL), incurs far less overhead than maintaining a traditional lock manager, but it also tracks less total information about contention between transactions. Under high-contention workloads, this can result in reduced concurrency and poor CPU utilization. To ameliorate this problem, we also propose an optimization called selective contention analysis (SCA), which, only when needed, efficiently computes the most useful subset of the contention information that is tracked in full by traditional lock managers at all times.

Our experiments show that VLL dramatically reduces lock management overhead, both in the context of a traditional database system running on a single (multi-core) server, and when used in a distributed database system that partitions data across machines in a shared-nothing cluster.

In such partitioned systems, the distributed commit protocol (typically two-phase commit) is often the primary bottleneck, rather than the lock manager. However, recent work on deterministic database systems such as Calvin [36,37] has shown how two-phase commit can be eliminated for distributed transactions, increasing throughput by up to an order of magnitude, and consequently reintroducing the lock manager as a major bottleneck. Fortunately, deterministic database systems like Calvin lock all data for a transaction at the very start of executing the transaction. Since this element of Calvin's execution protocol satisfies VLL's lock request ordering requirement, VLL fits naturally into the design of deterministic systems. When we compare VLL (implemented within the Calvin framework) against Calvin's native lock manager, which uses the traditional design of a hash table of request queues, we find that VLL enables an even greater throughput advantage than that which Calvin has already achieved over traditional nondeterministic execution schemes in the presence of distributed transactions.

We also propose an extension of VLL (called VLLR) that locks ranges of rows rather than individual rows. Experiments show that VLLR outperforms two traditional range locking mechanisms.

### 中文译文

随着主内存价格持续下降，越来越多的事务处理应用会把其活跃数据集的大部分，甚至全部，始终保存在主内存中。由于磁盘 `IO` 不再是瓶颈，这极大提升了 `OLTP` 数据库系统的性能。

通常，一个瓶颈被移除之后，新的瓶颈就会出现。对于主内存数据库系统来说，一个常见瓶颈就是锁管理器，尤其是在高冲突工作负载下更为明显。有研究报告称，在主内存 `DBMS` 中，事务执行时间的 `16%-25%` 都花在与锁管理器交互上 [12]。但这些实验是在单核机器上完成的，锁数据结构之间并不存在物理竞争。其他研究表明，当多个核上的事务同时竞争访问锁管理器时，锁管理器的开销会更大 [14,22,29]。随着每台机器的核心数继续增长，锁管理器将更加成为性能瓶颈。

尽管不同数据库系统并不以完全统一的方式实现加锁协议，但最常见的锁管理器实现方式，是使用一个哈希表，将每个可加锁记录的主键映射到该记录对应的锁请求链表 [2,4,5,11,34]。这个链表通常前面还有一个锁头，用来跟踪该条目的当前锁状态。为了保证线程安全，锁头通常还会维护一个互斥量对象；在发出锁请求和释放锁之前，都必须先获取它，以保证对链表元素的插入和删除始终发生在临界区内。每次释放锁时，还需要遍历链表，以判断下一个应该继承该锁的请求是谁。

这些哈希表查找、闩锁获取和链表操作，本质上都只是主内存操作，因此对于那些要访问磁盘数据的事务来说，它们原本只是很小的一部分成本。然而在主内存数据库系统里，这些操作已经不能再忽略。锁管理器操作带来的额外内存访问、缓存未命中、`CPU` 周期消耗以及临界区开销，可能接近甚至超过事务实际业务逻辑的执行成本。进一步地，随着每台服务器的核心数和处理器数增加，并发度也随之提升，锁冲突也更严重，于是每把锁对应的事务请求链表会变得更长，而每次释放锁时遍历这条链表的成本也会随之上升。

因此，我们认为，在现代主内存数据库系统中，有必要重新审视锁管理器的设计。本文探索了对锁管理器的两项重大改动。第一，我们把所有锁信息从中心化的锁数据结构中移走，转而把锁信息与被加锁的原始数据放在一起，这一方向此前已有工作提出 [8]。例如，在主内存数据库中，一条元组记录可以附加一些额外的隐藏属性，用来保存该元组的行级锁信息。这样一次内存访问就能在同一条缓存行里同时取到数据和锁信息，从而有可能减少额外的缓存未命中。

第二，我们从锁数据结构中移除了“哪些事务当前还对某把锁有未完成请求”这一整类信息。因此，我们不再为每把锁维护一条请求链表，而是只使用一个简单的信号量来表示该锁尚未完成的请求数；或者更具体地说，使用两个信号量，分别记录读请求和写请求的数量。去掉锁管理器主体数据结构的大部分内容之后，系统就不再容易判断：当前拥有者释放锁之后，下一步究竟应该由哪个事务继承这把锁。我们的一个关键贡献，就是给出了解决这个问题的方法。核心思路是强制一个事务一次性请求它需要的全部锁，并按照各事务请求锁的顺序对它们进行排序。随后我们利用这个全局事务顺序，判断最近一次释放锁之后，哪个事务应该被解除阻塞并允许运行。

这两项技术结合起来，就是我们所说的超轻量锁机制 `VLL`。与维护传统锁管理器相比，它带来的开销要小得多，但与此同时，它跟踪的事务冲突信息也更少。在高冲突工作负载下，这会导致并发度下降和 `CPU` 利用率变差。为了缓解这个问题，我们还提出了一种称为选择性竞争分析 `SCA` 的优化。它只在必要时才启动，用高效方式计算传统锁管理器平时一直完整维护、而这里仅需按需恢复的那一小部分最有用的冲突信息。

实验表明，不管是在单台多核服务器上的传统数据库系统中，还是在采用 `shared-nothing` 集群、数据跨机器分区的分布式数据库中，`VLL` 都能显著降低锁管理开销。

在这种分区式系统中，主要瓶颈通常并不是锁管理器，而是分布式提交协议，通常也就是两阶段提交 `2PC`。然而，最近关于 `Calvin` 一类确定性数据库系统的研究 [36,37] 表明，分布式事务中的两阶段提交是可以被消除的，这能把吞吐量提升一个数量级左右，同时又会让锁管理器重新成为主要瓶颈。幸运的是，像 `Calvin` 这样的确定性数据库系统，会在事务执行一开始就为该事务锁住全部数据。`Calvin` 执行协议中的这一点，恰好满足 `VLL` 对锁请求顺序的要求，所以 `VLL` 与确定性系统的设计天然契合。当我们把 `VLL` 嵌入 `Calvin` 框架，并将其与 `Calvin` 自带的原生锁管理器比较时，后者仍然采用传统的“哈希表 + 请求队列”设计。结果发现，在存在分布式事务的情况下，`VLL` 带来的吞吐优势，甚至超过了 `Calvin` 相对传统非确定性执行方案本身已经取得的优势。

我们还提出了 `VLL` 的一个扩展版本 `VLLR`，它锁住的是一段行范围，而不是单条记录。实验结果显示，`VLLR` 优于两种传统的范围锁机制。

## 2.1 The VLL algorithm

### Original

The biggest difference between VLL and traditional lock manager implementations is that VLL stores each record's "lock table entry" not as a linked list in a separate lock table, but rather as a pair of integer values (`C_X`, `C_S`) immediately preceding the record's value in storage, which represents the number of transactions requesting exclusive and shared locks on the record, respectively. When no transaction is accessing a record, its `C_X` and `C_S` values are both `0`.

In addition, a global queue of transaction requests (called `TxnQueue`) is kept at each partition, tracking all active transactions in the order in which they requested their locks.

When a transaction arrives at a partition, it attempts to request locks on all records at that partition that it will access in its lifetime. Each lock request takes the form of incrementing the corresponding record's `C_X` or `C_S` value, depending whether an exclusive or shared lock is needed. Exclusive locks are considered to be granted to the requesting transaction if `C_X = 1` and `C_S = 0` after the request, since this means that no other shared or exclusive locks are currently held on the record. Similarly, a transaction is considered to have acquired a shared lock if `C_X = 0`, since that means that no exclusive locks are held on the record.

Once a transaction has requested its locks, it is added to the `TxnQueue`. Both the requesting of the locks and the adding of the transaction to the queue happen inside the same critical section (so that only one transaction at a time within a partition can go through this step). In order to reduce the size of the critical section, the transaction attempts to figure out its entire read set and write set in advance of entering this critical section. This process is not always trivial and may require some exploratory actions. Furthermore, multi-partition transaction lock requests have to be coordinated. This process is discussed further in Sect. 2.4.

Upon leaving the critical section, VLL decides how to proceed based on two factors:

- Whether or not the transaction is local or distributed. A local transaction is one whose read and write sets include records that all reside on the same partition; distributed transactions may access a set of records spanning multiple data partitions.
- Whether or not the transaction successfully acquired all of its locks immediately upon requesting them. Transactions that acquire all locks immediately are termed free. Those which fail to acquire at least one lock are termed blocked.

VLL handles each transaction differently based on whether they are free or blocked:

- Free transactions are immediately executed. Once completed, the transaction releases its locks (i.e., it decrements every `C_X` or `C_S` value that it originally incremented) and removes itself from the `TxnQueue`. Note, however, that if the free transaction is distributed, then it may have to wait for remote read results, and therefore may not complete immediately.
- Blocked transactions cannot execute fully, since not all locks have been acquired. Instead, these are tagged in the `TxnQueue` as blocked. Blocked transactions are not allowed to begin executing until they are explicitly unblocked by the VLL algorithm.

In short, all transactions, free and blocked, local and distributed, are placed in the `TxnQueue`, but only free transactions begin execution immediately.

Since there is no lock management data structure to record which transactions are waiting for data locked by other transactions, there is no way for a transaction to hand over its locks directly to another transaction when it finishes. An alternative mechanism is therefore needed to determine when blocked transactions can be unblocked and executed. One possible way to accomplish this is for a background thread to examine each blocked transaction in the `TxnQueue` and examine the `C_X` and `C_S` values of each data item for which the transaction requested a lock. If the transaction incremented `C_X` for a particular item, and now `C_X` is down to `1` and `C_S` is `0` for that item, then the transaction clearly has an exclusive lock on it. Similarly, if the transaction incremented `C_S` and now `C_X` is down to `0`, the transaction has a shared lock on the item. If all data items that it requested are now available, the transaction can be unblocked and executed.

The problem with this approach is that if another transaction entered the `TxnQueue` and incremented `C_X` for the same data item that a transaction blocked in the `TxnQueue` already incremented, then both transactions will be blocked forever since `C_X` will always be at least `2`.

Fortunately, this situation can be resolved by a simple observation: a blocked transaction that reaches the front of the `TxnQueue` will always be able to be unblocked and executed, no matter how large `C_X` and `C_S` are for the data items it accesses. To see why this is the case, note that each transaction requests all locks and enters the queue all within the same critical section. Therefore, if a transaction makes it to the front of the queue, this means that all transactions that requested their locks before it have now completed. Furthermore, all transactions that requested their locks after it will be blocked if their read and write set conflict.

Since the front of the `TxnQueue` can always be unblocked and run to completion, every transaction in the `TxnQueue` will eventually be able to be unblocked. Therefore, in addition to reducing lock manager overhead, this technique also guarantees that there will be no deadlock within a partition. Note that a blocked transaction now has two ways to become unblocked: either it makes it to the front of the queue (meaning that all transactions that requested locks before it have finished completely), or it becomes the only transaction remaining in the queue that requested locks on each of the keys in its read set and write set. We discuss a more sophisticated technique for unblocking transactions in Sect. 2.6.

One problem that VLL sometimes faces is that as the `TxnQueue` grows in size, the probability of a new transaction being able to immediately acquire all its locks decreases, since the transaction can only acquire its locks if it does not conflict with any transaction in the entire `TxnQueue`.

We therefore artificially limit the number of transactions that may enter the `TxnQueue`. If the size exceeds a threshold, the system temporarily ceases to process new transactions, and shifts its processing resources to finding transactions in the `TxnQueue` that can be unblocked. In practice, we have found that this threshold should be tuned depending on the contention ratio of the workload. High-contention workloads run best with smaller `TxnQueue` size limits since the probability of a new transaction not conflicting with any element in the `TxnQueue` is smaller. A longer `TxnQueue` is acceptable for lower-contention workloads. In order to automatically account for this tuning parameter, we set the threshold not by the size of the `TxnQueue`, but rather by the number of blocked transactions in the `TxnQueue`, since high-contention workloads will reach this threshold sooner than low-contention workloads.

### 中文译文

`VLL` 与传统锁管理器实现方式最大的不同在于：`VLL` 不再把每条记录的“锁表项”作为独立锁表中的链表来保存，而是把它表示成两个紧挨在记录值之前存放的整数值 `C_X` 和 `C_S`，分别表示请求该记录排他锁和共享锁的事务数量。当没有事务访问某条记录时，它的 `C_X` 和 `C_S` 都为 `0`。

此外，每个分区还维护一个全局事务请求队列，称为 `TxnQueue`，用于按照事务请求锁的顺序记录所有活跃事务。

当一个事务到达某个分区时，它会尝试一次性请求该分区中自己整个生命周期内将要访问的全部记录的锁。每一次锁请求都体现为对相应记录的 `C_X` 或 `C_S` 加一，具体取决于需要的是排他锁还是共享锁。如果请求之后满足 `C_X = 1` 且 `C_S = 0`，那么这个事务就被认为已经获得了排他锁，因为这说明当前没有其他共享锁或排他锁持有该记录。类似地，如果请求之后 `C_X = 0`，则说明当前没有任何排他锁持有该记录，该事务就被认为已经获得了共享锁。

当事务完成全部锁请求之后，它会被加入 `TxnQueue`。请求锁和把事务加入队列，这两个动作都发生在同一个临界区中，因此在一个分区内，同一时刻只有一个事务能完成这一步。为了尽量缩小临界区，事务会在进入临界区之前，先尽量推导出完整的读集合和写集合。这个过程并不总是简单的，有时需要先做一些探索性动作。此外，多分区事务的加锁请求还需要跨分区协调。这个问题会在 `2.4` 节进一步讨论。

离开临界区之后，`VLL` 会根据两个因素决定后续处理方式：

- 事务是本地事务还是分布式事务。本地事务的读写集合全部位于同一个分区；分布式事务则可能访问跨多个数据分区的记录集合。
- 事务在请求锁时是否立刻成功拿到了全部锁。那些立即拿到全部锁的事务被称为 `free`；只要有至少一把锁没有立即获取成功，就被称为 `blocked`。

`VLL` 会根据事务是 `free` 还是 `blocked` 做不同处理：

- `free` 事务会立刻执行。执行完成后，该事务会释放自己持有的锁，也就是把之前增加过的每一个 `C_X` 或 `C_S` 再减回去，并把自己从 `TxnQueue` 中移除。不过需要注意的是，如果这个 `free` 事务是分布式事务，那么它可能还需要等待远端读结果，因此未必能立即完成。
- `blocked` 事务由于并没有拿到全部锁，不能完整执行。它会在 `TxnQueue` 中被标记为 `blocked`，只有在 `VLL` 算法显式将其解除阻塞之后，它才允许开始执行。

简言之，不论是 `free` 还是 `blocked`，也不论是本地事务还是分布式事务，都会进入 `TxnQueue`；只是只有 `free` 事务会立即开始执行。

由于 `VLL` 中不存在一个显式的锁管理数据结构来记录“哪些事务正在等待其他事务持有的数据”，所以一个事务在完成时，无法像传统锁管理器那样直接把锁交接给另一个事务。因此，系统必须有一种替代机制，来判断什么时候可以解除某个 `blocked` 事务的阻塞并执行它。一种可能的方法是：让后台线程检查 `TxnQueue` 中每个 `blocked` 事务，再查看该事务请求过锁的每个数据项当前的 `C_X` 和 `C_S`。如果某个数据项上，这个事务此前增加的是 `C_X`，而现在该项的 `C_X` 已降到 `1` 且 `C_S` 为 `0`，那么很显然，它已经独占了这条记录。同理，如果这个事务增加的是 `C_S`，而现在 `C_X` 已经降到 `0`，那么它已经获得了该项的共享锁。只要它请求的所有数据项现在都可用，这个事务就可以被解除阻塞并执行。

这种方法的问题在于：如果另一个事务后来进入 `TxnQueue`，并对同一个数据项的 `C_X` 再次加一，而队列中原本那个 `blocked` 事务也已经对它加过一，那么这两个事务就会永远都被阻塞，因为 `C_X` 将始终至少为 `2`。

幸运的是，这个问题可以通过一个简单观察来解决：任何一个到达 `TxnQueue` 队首的 `blocked` 事务，总是可以被解除阻塞并执行，无论它访问的数据项当前的 `C_X` 和 `C_S` 有多大。原因在于，每个事务请求全部锁并进入队列的动作，都发生在同一个临界区内。因此，如果一个事务已经来到队首，就意味着所有比它更早请求锁的事务都已经完成了。与此同时，任何在它之后请求锁、且与其读写集合冲突的事务，都会被阻塞住。

由于 `TxnQueue` 队首上的事务总能被解除阻塞并最终运行完成，因此队列中的每个事务最终都能被解除阻塞。换句话说，除了降低锁管理开销以外，这个技巧还保证了分区内部不会发生死锁。此时，一个 `blocked` 事务有两种方式变为可运行：要么它走到了队首，也就是所有比它更早请求锁的事务都已经彻底结束；要么它成为队列中唯一一个仍然对其读写集合中各个键请求过锁的事务。更复杂的解阻塞技术会在 `2.6` 节讨论。

`VLL` 有时会遇到一个问题：随着 `TxnQueue` 越来越长，一个新事务能够立刻拿到全部锁的概率会持续下降，因为它只有在与整个 `TxnQueue` 中任何事务都不冲突时，才能立即获取全部锁。

因此，我们会人为限制允许进入 `TxnQueue` 的事务数量。如果队列大小超过某个阈值，系统就会暂时停止处理新的事务，而把处理资源转去寻找队列中哪些事务已经可以解除阻塞。实践中，我们发现这个阈值应当根据工作负载的冲突比例来调节。高冲突工作负载更适合较小的 `TxnQueue` 大小限制，因为新事务与队列中任一事务都不冲突的概率更低；低冲突工作负载则可以接受更长的 `TxnQueue`。为了让这个调参过程更自动化，我们最终不是按 `TxnQueue` 的总长度设置阈值，而是按队列中 `blocked` 事务的数量设置阈值，因为高冲突工作负载会比低冲突工作负载更早触发这个条件。

## 2.4 Impediments to acquiring all locks at once

### Original

As discussed in Sect. 2.1, in order to guarantee that the head of the `TxnQueue` is always eligible to run (which has the added benefit of eliminating deadlocks), VLL requires that all locks for a transaction be acquired together in a critical section. There are two possibilities that make this nontrivial:

- The read and write sets of a transaction may not be known before running the transaction. An example of this is a transaction that updates a tuple that is accessed through a secondary index lookup. Without first doing the lookup, it is hard to predict what records the transaction will access, and therefore what records it must lock.
- Since each partition has its own `TxnQueue` and the critical section in which it is modified is local to a partition, different partitions may not begin processing transactions in the same order. This could lead to distributed deadlock, where one partition gets all its locks and activates a transaction, while that transaction is blocked in the `TxnQueue` of another partition.

In order to overcome the first problem, before the transaction enters the critical section, we allow the transaction to perform whatever reads it needs to, at no isolation, for it to figure out what data it will access (for example, it performs the secondary index lookups). After performing these exploratory reads, it enters the critical section and requests those locks that it discovered it would likely need. Once the transaction gets its locks and is handed off to an execution thread, the transaction runs as normal unless it discovers that it does not have a lock for something it needs to access (this could happen if, for example, the secondary index was updated immediately after the exploratory lookup was performed and now returns a different value). In such a scenario, the transaction aborts, releases its locks, and submits itself to the database system to be restarted as a completely new transaction.

There are two possible solutions to the second problem. The first is simply to allow distributed deadlocks to occur and to run a deadlock detection protocol that aborts deadlocked transactions. The second approach is to coordinate across partitions to ensure that multi-partition transactions are added to the `TxnQueue` in the same order on each partition.

As will be discussed further in Sect. 3, we tried both approaches and found that for high-contention workloads, the first solution is problematic, since the overhead of handling and detecting distributed deadlock completely negates the VLL advantage of reducing the overhead of lock management. Meanwhile, the second approach, while adding nontrivial coordination overhead, is still able to yield improved performance. For low-contention workloads, both approaches are possibilities.

However, recent work on deterministic database systems [35-37] shows that the coordination overhead of the second approach can be reduced by performing it before beginning transactional execution. In short, deterministic database systems such as Calvin order all transactions across partitions, and this order can be leveraged by VLL to avoid distributed deadlock. Furthermore, since deterministic systems have been shown to be a particularly promising approach in main memory database systems [35], the integration of VLL and deterministic database systems seems to be a particularly good match. We therefore used the second approach with coordination happening before transaction execution for our implementation in most experiments of this paper.

### 中文译文

如 `2.1` 节所述，为了保证 `TxnQueue` 队首上的事务始终具备运行资格，并顺带消除死锁，`VLL` 要求一个事务的全部锁必须在同一个临界区内一次性获取完成。不过，这件事并不总是容易做到，主要有两个原因：

- 事务的读集合和写集合在事务真正运行之前，未必已经知道。一个典型例子是：事务要更新一条通过二级索引查找到的元组。如果不先完成这次查找，就很难预测事务究竟会访问哪些记录，也就无法事先知道该给哪些记录加锁。
- 每个分区都有自己的 `TxnQueue`，而修改它的临界区也是分区本地的，因此不同分区并不一定会以相同顺序开始处理事务。这就可能导致分布式死锁：某个事务在一个分区上拿齐了所有锁并被激活，但在另一个分区的 `TxnQueue` 中却仍然处于阻塞状态。

为了解决第一个问题，系统允许事务在进入临界区之前，以“不提供隔离性”的方式先做它所需要的读取，以此推断自己将访问哪些数据，例如先执行二级索引查找。完成这些探索性读取后，事务才进入临界区，请求那些它判断自己大概率需要的锁。之后，一旦事务拿到这些锁并被交给执行线程，它就按正常方式运行；除非它在执行中发现，自己实际要访问的某项数据并没有对应的锁。这种情况是可能发生的，例如刚才探索性查找完成之后，二级索引立刻又被更新了，导致正式执行时返回了不同结果。在这种场景下，事务会中止、释放已拿到的锁，并把自己重新提交给数据库系统，作为一个全新的事务重新启动。

对于第二个问题，有两种解决办法。第一种是允许分布式死锁发生，然后运行死锁检测协议，把发生死锁的事务中止掉。第二种则是跨分区进行协调，确保多分区事务会以相同顺序被加入每个分区的 `TxnQueue`。

正如论文第 `3` 节进一步讨论的那样，作者尝试了这两种方法，并发现对于高冲突工作负载来说，第一种方案问题很大，因为分布式死锁的检测与处理开销，会完全抵消 `VLL` 在降低锁管理开销方面的优势。相比之下，第二种方案虽然引入了不小的协调成本，但总体上仍能带来性能提升。对于低冲突工作负载，这两种方案都可以考虑。

不过，最近关于确定性数据库系统的研究 [35-37] 表明，如果把这种协调动作提前到事务正式执行之前完成，那么第二种方案的协调开销可以显著降低。简而言之，像 `Calvin` 这样的确定性数据库系统，会预先对跨分区事务做全局排序，而 `VLL` 正好可以利用这个顺序来避免分布式死锁。并且，由于确定性系统已经被证明是主内存数据库系统里一种很有前景的路线 [35]，所以 `VLL` 与确定性数据库系统的结合显得尤其自然。正因为如此，在本文的大多数实验实现中，作者采用的都是第二种方案，即在事务执行之前就先完成跨分区协调。

## 2.5 Trade-offs of VLL

### Original

VLL's primary strength lies in its extremely low overhead in comparison with that of traditional lock management approaches. VLL essentially "compresses" a standard lock manager's linked list of lock requests into two integers. Furthermore, by placing these integers inside the tuple itself, both the lock information and the data itself can be retrieved with a single memory access, minimizing total cache misses.

The main disadvantage of VLL is a potential loss in concurrency. Traditional lock managers use the information contained in lock request queues to figure out whether a lock can be granted to a particular transaction. Since VLL does not have these lock queues, it can only test more selective predicates on the state: (a) whether this is the only lock in the queue, or (b) whether it is so old that it is impossible for any other transaction to precede it in any lock queue.

As a result, it is common for scenarios to arise under VLL where a transaction cannot run even though it "should" be able to run (and would be able to run under a standard lock manager design). Consider, for example, the sequence of transactions:

| txn | Write set |
| --- | --- |
| A | x |
| B | y |
| C | x, z |
| D | z |

Suppose A and B are both running in executor threads (and are therefore still in the `TxnQueue`) when C and D come along. Since transaction C conflicts with A on record x and D conflicts with C on z, both are put on the `TxnQueue` in blocked mode. VLL's "lock table state" would then look like the following (as compared to the state of a standard lock table implementation):

| VLL Key | `C_X` | `C_S` | Standard Key | Request queue |
| --- | --- | --- | --- | --- |
| x | 2 | 0 | x | A, C |
| y | 1 | 0 | y | B |
| z | 2 | 0 | z | C, D |
| `TxnQueue` | A, B, C, D |  |  |  |

Next, suppose that A completes and releases its locks. The lock tables would then appear as follows:

| VLL Key | `C_X` | `C_S` | Standard Key | Request queue |
| --- | --- | --- | --- | --- |
| x | 1 | 0 | x | C |
| y | 1 | 0 | y | B |
| z | 2 | 0 | z | C, D |
| `TxnQueue` | B, C, D |  |  |  |

Since C appears at the head of all its request queues, a standard implementation would know that C could safely be run, whereas VLL is not able to determine that.

When contention is low, this inability of VLL to immediately determine possible transactions that could potentially be unblocked is not costly. However, under higher contention workloads, and especially when there are distributed transactions in the workload, VLL's resource utilization suffers, and additional optimizations are necessary. We discuss such optimizations in the next section.

### 中文译文

`VLL` 最主要的优势，在于它相对于传统锁管理方法拥有极低的运行开销。`VLL` 本质上是把标准锁管理器中“每把锁对应一条请求链表”的表示方式，压缩成了两个整数。进一步地，由于这两个整数被直接放在元组本体中，系统只需一次内存访问，就能同时取到锁信息和数据本身，从而尽量减少总的缓存未命中。

`VLL` 的主要缺点，是它可能会损失一部分并发性。传统锁管理器依靠锁请求队列中的信息，判断某把锁是否可以授予某个特定事务。由于 `VLL` 没有这些锁队列，所以它只能检查更受限的条件：`(a)` 这个锁请求是不是队列中的唯一请求；或者 `(b)` 这个事务是不是已经足够老，以至于不可能再有其他事务排在它之前。

因此，在 `VLL` 中，经常会出现这样一种情况：某个事务其实“本来应该可以运行”，而且在标准锁管理器设计下也确实可以运行，但 `VLL` 却无法判断出来。以下面这个事务序列为例：

| 事务 | 写集合 |
| --- | --- |
| A | x |
| B | y |
| C | x, z |
| D | z |

假设当事务 `C` 和 `D` 到来时，`A` 与 `B` 都还在执行线程中运行，因此它们仍然留在 `TxnQueue` 中。由于事务 `C` 在记录 `x` 上与 `A` 冲突，而事务 `D` 在 `z` 上与 `C` 冲突，所以 `C` 和 `D` 都会以 `blocked` 状态被放入 `TxnQueue`。这时，`VLL` 的“锁表状态”会如下所示，同时右侧给出标准锁表实现下的状态：

| VLL 的键 | `C_X` | `C_S` | 标准锁表的键 | 请求队列 |
| --- | --- | --- | --- | --- |
| x | 2 | 0 | x | A, C |
| y | 1 | 0 | y | B |
| z | 2 | 0 | z | C, D |
| `TxnQueue` | A, B, C, D |  |  |  |

接着，假设 `A` 执行完成并释放了它的锁，此时两种实现下的锁表会变成下面这样：

| VLL 的键 | `C_X` | `C_S` | 标准锁表的键 | 请求队列 |
| --- | --- | --- | --- | --- |
| x | 1 | 0 | x | C |
| y | 1 | 0 | y | B |
| z | 2 | 0 | z | C, D |
| `TxnQueue` | B, C, D |  |  |  |

由于 `C` 现在已经位于它所有请求队列的队首，标准实现能够判断出：`C` 已经可以安全运行。但 `VLL` 并没有足够的信息得出这个结论。

在低冲突场景下，`VLL` 不能立刻识别出“哪些事务其实已经可以被解锁并执行”这件事，代价并不高。但在更高冲突的工作负载下，尤其是工作负载中还包含分布式事务时，`VLL` 的资源利用率会明显下降，因此就需要额外的优化。下一节讨论的正是这种优化。

## 2.6 Selective contention analysis (SCA)

### Original

For high-contention and high-percentage multi-partition workloads, VLL spends a growing percentage of CPU cycles in the state described in Sect. 2.5 above, where no transaction can be found that is known to be safe to execute, whereas a standard lock manager would have been able to find one. In order to maximize CPU resource utilization, we introduce the idea of SCA.

SCA simulates the standard lock manager's ability to detect which transactions should inherit released locks. It does this by spending work examining contention, but only when CPUs would otherwise be sitting idle (i.e., `TxnQueue` is full and there are no obviously unblockable transactions). SCA therefore enables VLL to selectively increase its lock management overhead when, and only when, it is beneficial to do so.

Any transaction in the `TxnQueue` that is in the `blocked` state conflicted with one of the transactions that preceded it in the queue at the time that it was added. Since then, however, the transaction or transactions that caused it to become blocked may have completed and released their locks. As the transaction gets closer and closer to the head of the queue, it therefore becomes much less likely to be "actually" blocked.

In general, the `i`th transaction in the `TxnQueue` can only conflict now with up to `(i - 1)` prior transactions, whereas it previously had to contend with up to the number of `TxnQueueSizeLimit` prior transactions. Therefore, SCA starts at the front of the queue and works its way through the queue looking for a transaction to execute. The whole while, it keeps two bit arrays, `D_X` and `D_S`, each of size `100 kB` (so that both will easily fit inside an `L2` cache of size `256 kB`) and initialized to all `0`s. SCA then maintains the invariant that after scanning the first `i` transactions:

- `D_X[j] = 1` iff an element of one of the scanned transactions' write sets hashes to `j`
- `D_S[k] = 1` iff an element of one of the scanned transactions' read sets hashes to `k`

Therefore, if at any point the next transaction scanned (let's call it `T_next`) has the properties:

- `D_X[hash(key)] = 0` for all keys in `T_next`'s read set
- `D_X[hash(key)] = 0` for all keys in `T_next`'s write set
- `D_S[hash(key)] = 0` for all keys in `T_next`'s write set

then `T_next` does not conflict with any of the prior scanned transactions and can safely be run.

In other words, SCA traverses the `TxnQueue` starting with the oldest transactions and looking for a transaction that is ready to run and does not conflict with any older transaction.

SCA is actually "selective" in two different ways. First, it only gets activated when it is really needed, in contrast to traditional lock manager overhead which always pays the cost of tracking lock contention even when this information will not end up being used. Second, rather than doing an expensive all-to-all conflict analysis between active transactions, which is what traditional lock managers track at all times, SCA is able to limit its analysis to those transactions that are `(a)` most likely to be able to run immediately and `(b)` least expensive to check.

In order to improve the performance of our implementation of SCA, we include a minor optimization that reduces the CPU overhead of running SCA. Each key needs to be hashed into the `100 kB` bitstring, but hashing every key for each transaction as we iterate through the `TxnQueue` can be expensive. We therefore cache, inside the transaction state, the results of the hash function the first time SCA encounters a transaction. If that transaction is still in the `TxnQueue` the next time SCA iterates through the queue, the algorithm may then use the saved list of offsets that corresponds to the keys read and written by that transaction to set the appropriate bits in the SCA bitstring, rather than having to re-hash each key.

### 中文译文

对于高冲突、且多分区事务占比较高的工作负载，`VLL` 会花越来越多的 `CPU` 周期停留在 `2.5` 节描述的那种状态中：系统找不到一个“已知可以安全执行”的事务，而标准锁管理器在这种情况下本来是能够找出一个的。为了最大化 `CPU` 资源利用率，作者提出了 `SCA`。

`SCA` 试图模拟标准锁管理器“判断哪些事务应该继承刚释放出来的锁”的能力。它的做法是：去分析冲突关系，但只在 `CPU` 否则会空闲的时候才做这件事，也就是 `TxnQueue` 已满、且系统又找不到那些显然可以被解除阻塞的事务时。于是，`SCA` 让 `VLL` 只在有收益的时候，才有选择地增加一部分锁管理开销。

任何一个在 `TxnQueue` 中处于 `blocked` 状态的事务，在它进入队列的那一刻，都必然与排在它前面的某个事务发生过冲突。不过，到了后续某个时刻，当初导致它阻塞的那个或那些事务，可能已经完成并释放了锁。因此，随着这个事务越来越接近队首，它“实际上仍然被阻塞”的可能性就会越来越低。

一般来说，`TxnQueue` 中第 `i` 个事务，在当前时刻最多只可能与它之前的 `i - 1` 个事务冲突；而在它刚进入系统时，它需要面对的潜在前驱事务数量，最多可能达到 `TxnQueueSizeLimit`。因此，`SCA` 会从队首开始，沿着队列往后扫描，寻找一个可执行事务。在整个扫描过程中，它维护两个位数组 `D_X` 和 `D_S`，每个都是 `100 kB` 大小，因此都可以轻松放进 `256 kB` 的 `L2` 缓存，并且初始值都为 `0`。`SCA` 维持如下不变式：在扫描完前 `i` 个事务之后，

- `D_X[j] = 1`，当且仅当某个已扫描事务的写集合中，有元素哈希到位置 `j`
- `D_S[k] = 1`，当且仅当某个已扫描事务的读集合中，有元素哈希到位置 `k`

因此，如果在某个时刻，接下来被扫描的事务，我们记作 `T_next`，满足以下条件：

- `T_next` 的读集合中所有键都满足 `D_X[hash(key)] = 0`
- `T_next` 的写集合中所有键都满足 `D_X[hash(key)] = 0`
- `T_next` 的写集合中所有键都满足 `D_S[hash(key)] = 0`

那么就说明 `T_next` 与之前已经扫描过的事务都不冲突，因此可以安全运行。

换句话说，`SCA` 会从最老的事务开始遍历 `TxnQueue`，寻找一个既已经准备好运行、又不会与任何更早事务发生冲突的事务。

`SCA` 之所以叫“selective”，实际上体现在两个层面。第一，它只在真的需要时才会激活；相比之下，传统锁管理器无论这些冲突信息最终是否会用到，都要持续付出维护它们的代价。第二，传统锁管理器本质上一直在维护活跃事务之间的全量两两冲突关系，而 `SCA` 不需要做这种昂贵的全局分析。它只把分析范围限制在两类事务上：`(a)` 最有可能马上就能运行的事务，以及 `(b)` 最便宜、最容易检查的事务。

为了进一步提升 `SCA` 实现的性能，论文还加入了一个小优化，用来减少运行 `SCA` 时的 `CPU` 开销。由于每个键都要被哈希进那个 `100 kB` 的位串里，如果每次遍历 `TxnQueue` 时都对每个事务的每个键重新做一次哈希，成本会比较高。因此，作者在 `SCA` 第一次遇到某个事务时，会把哈希函数结果缓存在该事务的状态里。如果下次 `SCA` 再遍历到这个事务，并且它仍然还在 `TxnQueue` 中，那么算法就可以直接使用之前保存下来的偏移列表，把相应的位设到 `SCA` 位串中，而不必重新对每个键逐个哈希。

## 5 Conclusion and future work

### Original

We have presented VLL, a protocol for main memory database systems that avoids the costs of maintaining the data structures kept by traditional lock managers, and therefore yields higher transactional throughput than traditional implementations. VLL colocates lock information (two simple semaphores) with the raw data, and forces all locks to be acquired by a transaction at once. Although the reduced lock information can complicate answering the question of when to allow blocked transactions to acquire locks and proceed, SCA allows transactional dependency information to be created as needed, and only as much information as is needed to unblock transactions. This optimization allows our VLL protocol to achieve high concurrency in transaction execution, even under high contention.

We showed how VLL can be implemented in traditional single-server multi-core database systems and also in deterministic multi-server database systems. The experiments we presented demonstrate that VLL can outperform standard two-phase locking, deterministic locking, and H-Store style serial execution schemes significantly, without inhibiting scalability or interfering with other components of the database system. Furthermore, VLL is highly compatible with both standard nondeterministic approaches to transaction processing and deterministic database systems like Calvin.

We also showed that VLL can be easily extended to support range locking by adding two extra counters and a prefix locking algorithm, and we call this technique VLLR. Experiments demonstrate that VLLR outperforms other alternative range locking approaches.

Our focus in this paper was on database systems that update data in place. In future work, we intend to investigate multi-versioned variants of the VLL protocol and integrate hierarchical locking approaches into VLL.

### 中文译文

本文提出了 `VLL`，这是一种面向主内存数据库系统的协议。它避免了维护传统锁管理器那些复杂数据结构所需的成本，因此能够获得高于传统实现的事务吞吐量。`VLL` 把锁信息，也就是两个简单的信号量，与原始数据放在一起，并强制事务一次性获取全部锁。虽然锁信息被压缩之后，会使“何时允许某个阻塞事务真正拿到锁并继续执行”这个问题变得更难回答，但 `SCA` 允许系统在需要时按需构造事务依赖信息，而且只构造足以解除阻塞所必需的那一部分信息。这个优化使得 `VLL` 即使在高冲突场景下，也仍然能够取得较高的事务执行并发度。

论文展示了如何在传统的单机多核数据库系统中实现 `VLL`，也展示了如何在确定性的多服务器数据库系统中实现它。实验结果表明，`VLL` 能显著优于标准两阶段加锁、确定性加锁，以及 `H-Store` 风格的串行执行方案，而且不会损害系统的可扩展性，也不会干扰数据库系统的其他组件。除此之外，`VLL` 既与标准的非确定性事务处理方式兼容，也与 `Calvin` 这类确定性数据库系统高度兼容。

论文还展示了，只需再增加两个计数器并配合一种前缀加锁算法，就可以很容易地把 `VLL` 扩展为支持范围锁的版本，这个技术被称为 `VLLR`。实验表明，`VLLR` 优于其他替代性的范围锁方法。

本文主要关注的是“原地更新数据”的数据库系统。未来的工作方向包括：研究 `VLL` 的多版本变体，以及把层次化加锁方法整合进 `VLL`。
