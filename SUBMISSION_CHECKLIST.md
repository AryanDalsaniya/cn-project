# Final submission review - Groups 097 and 112

| Assignment requirement | Package evidence / action |
|---|---|
| Correct archive and report names | `FinalSubmission_Group_097_112.zip`, `FinalReport_Group_097_112.pdf` |
| Four required source folders | Present with self-contained server/client source and Makefiles |
| `make` creates `server`, `client` in each folder | Verified by integration suite |
| Exact mandatory CLI, no extra required arguments | Verified; optional mode/worker parameters have defaults |
| Single- and multi-threaded Part I | Same binary, optional `single`/`multi` mode; default `multi` |
| Topic directory mapping from CLI root | Startup map, sorted regular-file manifests |
| Part II Case 1 restart | Implemented and fault-tested |
| Case 2 checkpoint/resume | Implemented; lost DATA ACK and terminal-message loss tested |
| Enhanced parallel connections | Implemented; K=1,2,4,8 tested/measured |
| Configurable probabilistic disconnection | Same post-ACK per-chunk Bernoulli trial in all Part II cases |
| Raw performance CSVs and graphs | `Results/Animals10` and `Results/Local` with repeat-level logs, summaries and PNG/PDF graphs |
| Throughput, useful/total/redundant bytes and overhead | Tables and figures distinguish payload from application bytes |
| Member names and roll numbers | Copied from the supplied Group 097/112 design |
| Actual individual contribution note | **Group must confirm/edit the six proposed role lines** |
| Implemented design and deviations | Final report sections on protocol, sessions, concurrency and deviations |
| Animals10 experiments | Complete cane topic measured on one host: 4,863 images, 87 runs, SHA-256 verified |
| Three computers: one server and two client nodes | **Not available here; run LAN workflow and add evidence** |
| Final academic review and viva preparation | Read all code, reproduce experiments, review permitted assistance/attribution |

The included local evidence is real, but this package should not be described as satisfying the required distributed experiment until the required physical topology experiment is completed. No screenshots or measurements from another group's work are used.
