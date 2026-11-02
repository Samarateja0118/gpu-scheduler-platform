Cluster: 16 nodes x 8 simulated GPUs (12 a100, 4 h100), 3 tenants, 72h arrival window, 5 seeds per cell. Values are means over seeds (min-max in brackets for GPU allocated). Waits are submission to first start, in minutes. GPUs are simulated; this measures the scheduling algorithm on a virtual clock.

### Offered load 0.70

| policy | jobs | GPU allocated | useful GPU util (minus preempted work) | idle GPUs while jobs queued | critical p95 wait (min) | standard p95 | batch p95 | 8-GPU p95 | preemptions | GPU-h lost to preemption | guarantee shortfall GPU-h | cycle p99 (us) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo-firstfit | 3914 | 68.3% [65.9-70.5] | 68.3% | 1.8% | 0.6 | 1.9 | 1.4 | 38.8 | 0 | 0 (0.0%) | 105 | 16 |
| priority-bestfit-nopreempt | 3914 | 68.3% [65.9-70.5] | 68.3% | 4.0% | 1.3 | 2.9 | 0.8 | 45.5 | 0 | 0 (0.0%) | 149 | 18 |
| priority-bestfit-preempt | 3914 | 70.4% [68.0-72.2] | 68.3% | 3.3% | 0.0 | 2.9 | 4.2 | 48.5 | 320 | 199 (3.1%) | 161 | 57 |
| priority-spread-preempt | 3914 | 73.3% [70.7-75.1] | 67.8% | 15.2% | 0.0 | 8.6 | 50.0 | 135.3 | 533 | 505 (8.0%) | 555 | 88 |

### Offered load 0.85

| policy | jobs | GPU allocated | useful GPU util (minus preempted work) | idle GPUs while jobs queued | critical p95 wait (min) | standard p95 | batch p95 | 8-GPU p95 | preemptions | GPU-h lost to preemption | guarantee shortfall GPU-h | cycle p99 (us) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo-firstfit | 4734 | 82.3% [80.6-85.2] | 82.3% | 3.8% | 17.2 | 29.0 | 35.7 | 148.5 | 0 | 0 (0.0%) | 425 | 26 |
| priority-bestfit-nopreempt | 4734 | 82.3% [80.6-85.2] | 82.3% | 6.2% | 4.9 | 22.2 | 81.9 | 317.0 | 0 | 0 (0.0%) | 539 | 33 |
| priority-bestfit-preempt | 4734 | 87.7% [84.7-90.8] | 81.8% | 6.3% | 0.0 | 35.2 | 124.7 | 379.7 | 1245 | 547 (7.1%) | 503 | 185 |
| priority-spread-preempt | 4734 | 87.1% [85.4-89.3] | 79.4% | 11.5% | 0.1 | 37.2 | 376.4 | 850.9 | 1475 | 717 (9.3%) | 753 | 226 |

### Offered load 1.00

| policy | jobs | GPU allocated | useful GPU util (minus preempted work) | idle GPUs while jobs queued | critical p95 wait (min) | standard p95 | batch p95 | 8-GPU p95 | preemptions | GPU-h lost to preemption | guarantee shortfall GPU-h | cycle p99 (us) |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fifo-firstfit | 5629 | 96.1% [94.5-97.0] | 96.1% | 2.9% | 142.8 | 265.1 | 326.8 | 478.9 | 0 | 0 (0.0%) | 990 | 145 |
| priority-bestfit-nopreempt | 5629 | 92.3% [91.0-93.0] | 92.3% | 6.8% | 13.1 | 147.2 | 1284.3 | 1782.5 | 0 | 0 (0.0%) | 924 | 152 |
| priority-bestfit-preempt | 5629 | 95.8% [95.3-96.6] | 86.9% | 3.3% | 2.9 | 167.7 | 1505.4 | 2448.3 | 3014 | 819 (9.0%) | 585 | 469 |
| priority-spread-preempt | 5629 | 94.0% [92.7-95.7] | 85.7% | 5.3% | 2.6 | 174.3 | 1799.8 | 2772.8 | 2900 | 773 (8.4%) | 712 | 467 |

