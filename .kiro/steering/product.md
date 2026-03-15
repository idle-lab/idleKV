# idleKV

idleKV is a research-oriented, high-performance in-memory key-value database targeting modern multi-core hardware. It is Redis protocol-compatible, meaning standard Redis clients can connect to it.

## Core Goals

- High throughput and low latency (>1M QPS, <1μs point query)
- Lock-free reads via MVCC + OCC concurrency control
- ART (Adaptive Radix Tree) as the primary index structure with SIMD optimization
- WAL + Snapshot for crash recovery and durability
- GPU-accelerated batch queries (CUDA, exploratory)
- Epoch-Based Reclamation (EBR) for safe memory management

## Current State

The project is in active development. Some features (e.g., config file parsing, GPU acceleration) are not yet implemented. The server accepts Redis RESP protocol connections and dispatches commands through a sharded engine.
