# Experiment Runlog

This log documents the iterative development and optimization of the reliable real-time UDP streaming system across network profiles.

---

### Run 1: Baseline (Unmodified Naive Forwarder)
- **Profile**: `profiles/A.json` (Mild loss: 2%, Delay: 10-40ms, Dup: 0.5%)
- **Target `delay_ms`**: 60 ms
- **Miss %**: ~2.50% (Failed - exceeds 1.0% limit)
- **Bandwidth Overhead**: 1.00x
- **Result**: `INVALID`
- **Changes & Rationale**: Naive `recvfrom() -> sendto()` without packet recovery or jitter buffering. Every dropped packet produces an immediate deadline miss.

---

### Run 2: Jitter Buffer & Playout Engine
- **Profile**: `profiles/A.json`
- **Target `delay_ms`**: 60 ms
- **Miss %**: ~2.10% (Failed)
- **Bandwidth Overhead**: 1.00x
- **Result**: `INVALID`
- **Changes & Rationale**: Added sequence-indexed jitter buffer and `playout_thread` scheduled at `T0 + DELAY_MS/1000 + i*0.020`. Successfully absorbs jitter and reordering, but lost packets are still missing.

---

### Run 3: Fast NACK Retransmissions + Packet Cache
- **Profile**: `profiles/A.json`
- **Target `delay_ms`**: 60 ms
- **Miss %**: 0.00% (Passed - well under 1.00%)
- **Bandwidth Overhead**: 1.05x (Passed - well under 2.00x cap)
- **Result**: `VALID`
- **Changes & Rationale**: Implemented thread-safe `FrameCache` in Sender (port 47010 -> 47001) and feedback thread listening on port 47004. Implemented overdue/missing frame detector in Receiver sending compact binary NACK packets (`0x01` + count + sequence array) over port 47003 -> 47004.

---

### Run 4: Stress Test on Moderate Hostile Profile B
- **Profile**: `profiles/B.json` (Moderate loss: 5%, Delay: 20-80ms, Dup: 1%)
- **Target `delay_ms`**: 60 ms
- **Miss %**: 0.13% (Passed - under 1.00%)
- **Bandwidth Overhead**: 1.08x (Passed - under 2.00x cap)
- **Result**: `VALID`
- **Changes & Rationale**: Added fast overdue detection (20ms post-emission NACK trigger) and 12ms re-NACK retry interval to recover from lost NACKs during burst losses.

---

### Run 5: Playout Delay Minimization Sweep
- **Profile**: `profiles/A.json`
- **Tested Delays**: `60ms`, `55ms`, `50ms`, `45ms`, `40ms`, `35ms`, `30ms`
- **Optimal Valid Target `delay_ms`**: **50 ms**
- **Miss %**: **0.67%** (Passed - under 1.00% cap)
- **Bandwidth Overhead**: **2.00x** (Passed - within 2.00x cap)
- **Result**: `VALID` (Lowest valid playout delay: **50 ms**)
- **Summary**: Playout delays of 45ms and lower resulted in >1.00% deadline misses (45ms: 1.13%, 40ms: 4.60%). Delay of 50ms is the minimum valid configuration that strictly satisfies both deadline miss rate (<1.00%) and bandwidth overhead (<=2.00x) criteria.

