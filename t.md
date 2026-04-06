## redis

```
Summary:
  throughput summary: 1631321.38 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.427     0.120     0.431     0.543     0.607     4.399

throughput summary: 2114165.00 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.863     0.080     0.879     1.015     1.087     2.167
```

9:1

```
=== Summary ===
duration_sec           : 10.000
total_requests         : 21089391
total_errors           : 0
throughput_req_per_sec : 2108929
tx_mib_per_sec         : 275.98
rx_mib_per_sec         : 1870.79
avg_req_latency_us     : 30.16
```

## dragonfly

```
Summary:
  throughput summary: 1894657.12 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.968     0.104     0.615     3.047     3.527     5.783
Summary:
  throughput summary: 1015744.00 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        1.291     0.104     0.959     3.279     3.711     8.479
```

=== Summary ===
duration_sec           : 10.001
total_requests         : 11771030
total_errors           : 0
throughput_req_per_sec : 1176968
tx_mib_per_sec         : 153.97
rx_mib_per_sec         : 1044.11
avg_req_latency_us     : 46.34

## idlekv

```
Summary:
  throughput summary: 1516990.25 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        1.125     0.024     1.039     2.191     2.839     5.287

throughput summary: 1978239.38 requests per second
  latency summary (msec):
          avg       min       p50       p95       p99       max
        0.943     0.320     0.799     1.799     2.455    24.751
```

=== Summary ===
duration_sec           : 10.000
total_requests         : 18088832
total_errors           : 0
throughput_req_per_sec : 1808875
tx_mib_per_sec         : 236.60
rx_mib_per_sec         : 8.63
avg_req_latency_us     : 33.72