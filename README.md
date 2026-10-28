# IT305 final project - Groups 097 and 112

**Fault-Tolerant Topic-based File Distribution Service**

This package implements the final phase of the submitted design: Part I (single and multi-threaded), Part II Case 1 (restart), Case 2 (checkpoint/resume), and Enhanced (parallel file transfers). The course-required CLI and directory names are preserved.

**Before submission:** replace the proposed contribution descriptions on the report cover with the six members' actual contributions. The main report now uses real **single-host TCP tests on the complete Animals10 cane topic** (4,863 images; 60,686,716 bytes). The three-computer topology remains untested: one server computer plus two client computers are still required by the assignment. Earlier synthetic results remain separately labelled in `Results/Local`.

## Contents

- `FinalReport_Group_097_112.pdf`: compiled report.
- `Report/main.tex`: self-contained editable Overleaf source with embedded tables and native PGFPlots charts. The 12-page layout follows the supplied reference report.
- `Part1/`, `Part2_Case1/`, `Part2_Case2/`, `Part2_Case2_Enhanced/`: self-contained C++17 source and Makefile in each folder.
- `Results/Animals10/`: 87 measured runs on the complete real-image topic, raw logs, SHA-256 manifest, archive provenance, environment, summaries and PNG/PDF graphs.
- `Results/Local/`: 87 earlier synthetic baseline runs, retained separately.
- `Results/Validation/`: integration-test results and connection logs.
- `Tools/`: dataset generator, integration tests, local benchmarks, LAN orchestration and plotting.
- `DEMO_AND_VIVA.md`: demonstration commands and explanations.
- `SUBMISSION_CHECKLIST.md`: exact requirements and outstanding external work.

## Build (Linux / WSL Ubuntu)

Prerequisites: `g++` with C++17, GNU Make, POSIX sockets and pthreads. Python 3.9+ runs the test tools; Matplotlib is needed only to regenerate graphs. Program execution has no Python or third-party C++ dependency.

```bash
make -C Part1
make -C Part2_Case1
make -C Part2_Case2
make -C Part2_Case2_Enhanced
```

Each command creates exactly two executables **in that directory**: `server` and `client`. Binaries are intentionally omitted from the ZIP; build them on the target computer. Source files are repeated in each case to keep each folder independently buildable. `CASE` in its Makefile selects the implementation. Edit the matching common files consistently if changing the protocol.

## Required commands

All paths, IP addresses, ports and topic names below are examples to replace for your machine. The programs do not hard-code them. Topic matching is case-sensitive: use exactly the directory name, for example `cane` if Animals10 uses lowercase names.

Part I server, default multi-threaded:

```bash
./Part1/server 9000 /absolute/path/to/raw-img
```

Part I single-threaded comparison (optional trailing argument):

```bash
./Part1/server 9000 /absolute/path/to/raw-img single
./Part1/client 127.0.0.1 9000 cane /absolute/path/to/downloads
```

Stop the first server before starting another on the same port. In separate terminals, for each Part II case:

```bash
./Part2_Case1/server 9000 /absolute/path/to/raw-img 0.001
./Part2_Case1/client 127.0.0.1 9000 cane /absolute/path/to/case1-output

./Part2_Case2/server 9000 /absolute/path/to/raw-img 0.001
./Part2_Case2/client 127.0.0.1 9000 cane /absolute/path/to/case2-output

./Part2_Case2_Enhanced/server 9000 /absolute/path/to/raw-img 0.001
./Part2_Case2_Enhanced/client 127.0.0.1 9000 cane /absolute/path/to/enhanced-output
```

The default enhanced client opens four logical worker connections. Each worker reconnects independently. Every client writes to `<output_dir>/<topic>/`. Use a fresh output directory for each client/experiment; two processes must not write into the same topic directory.

Optional syntax:

```text
Part1/server <port> <topics_root_dir> [single|multi [seed]]
Part2*/server <port> <topics_root_dir> <failure_probability> [seed]
client <server_ip> <server_port> <topic> <output_dir> [connections [max_attempts]]
```

`connections` defaults to 4 in Enhanced, 1 elsewhere; only Enhanced accepts values other than 1 (range 1-32). `max_attempts` defaults to 1000 per worker. Example: `./Part2_Case2_Enhanced/client 127.0.0.1 9000 cane /tmp/demo 2 200`. IPv4 addresses are supported; DNS names and IPv6 are not part of this implementation. The server accepts up to 256 active worker threads. A 30-second socket timeout bounds stalled I/O. SIGINT/SIGTERM stops acceptance and waits for active workers to close/timeout.

## Fault injection and checkpoints

- Files are transferred in chunks of up to 65,536 bytes. Every DATA frame includes its file index, offset and CRC32.
- After a complete DATA chunk is acknowledged, the server makes one independent Bernoulli decision with probability `p`. On failure it closes that worker's connection, including after the last chunk of a file. A seed optionally fixes pseudorandom streams; concurrent connection ordering can still vary.
- `p=0` disables injection. `p=1` prevents a nonempty restart transfer from completing; bounded retries return failure. Resume can still progress because the chunk was checkpointed before the injected close. Empty topics have no DATA decision points.
- Case 1 discards progress and removes manifest-owned destination files before a fresh START. Every attempt starts with the first file. No transfer checkpoint survives reconnect.
- Case 2 stores session identity, topic, assigned worker, file index and offset in both processes. The server also records the last offered successor cursor. A reconnection may claim exactly the committed cursor or that offered cursor, covering an ACK lost after the client wrote data. No arbitrary offset is accepted.
- FILE_END is also acknowledged, including for zero-byte files. Loss of FILE_END, its ACK, or TRANSFER_END is recoverable.
- Enhanced assigns sorted file index `i` to worker `i % K`. A file has only one writer. Different workers have independent sessions and retries.
- Checkpoints are **in memory** and protect against connection failures. They do not survive client/server process restart or power loss. Writes are not fsynced per chunk. Completed/idle server sessions are retained for 600 seconds and reclaimed on subsequent requests; the table is limited to 10,000 sessions.
- Keep source files unchanged during a server run. Topics and sorted manifests are discovered at startup. Source size, inode and modification time are checked before opening each transfer file. Add/change topics only by restarting the server.

## Tests and reproducible local results

```bash
python3 Tools/test_project.py
python3 Tools/benchmark.py
python3 Tools/plot_results.py
```

The integration tests build all cases, start actual TCP server/client processes, use SHA-256 to compare file sets, and exercise normal transfer, empty/binary files, filenames with spaces/Unicode, invalid input, concurrent clients, probabilistic failures, p=1, lost acknowledgements, partial frames, terminal-message loss and destination-symlink refusal. A failed transfer returns a nonzero exit status.

The local benchmark generates an identical 2 MiB topic of eight binary files, uses 3 repetitions, compares 1/2/4/8 concurrent Part I clients, varies Part II failure probability and compares K=1/2/4/8. Every completed client is SHA-256 checked. Raw logs are saved before summaries/plots are calculated. Use `--extend` only to finish missing configurations for the same dataset/settings; otherwise use a new results directory. Results are stochastic measurements, not exact deterministic performance guarantees.

An existing dataset can be used locally:

```bash
python3 Tools/benchmark_animals10.py --topics /absolute/path/to/raw-img --topic cane --results Results/Animals10_Local
python3 Tools/plot_results.py --results Results/Animals10_Local
```

The Animals10 wrapper selects lower failure probabilities for large topics before measurement. For custom sweeps, use `Tools/benchmark.py --probabilities ... --parallel-probability ...`, choosing a parallel-test rate present in the sweep. Case 1 requires one uninterrupted traversal and can take extremely long when the number of chunks is large. This is a property of restart recovery, not evidence that the resume variant should use a different fault policy.

Counters: `payload_sent` counts file bytes accepted by successful server `send()` calls, including repetitions. `useful_bytes` counts each source file once per successful client. `redundant_bytes = payload_sent - useful_bytes`. `application_bytes` sums outgoing bytes at both endpoints, including headers and ACKs; `control_bytes = application_bytes - payload_sent`. Ethernet/IP/TCP headers and TCP retransmissions are **not** measured. Aggregate goodput is total useful bytes divided by common batch wall time; sender rate uses payload sent instead. MiB = 1,048,576 bytes. Integrity checking is outside the measured interval.

## Actual Animals10 measurements included

The report uses the entire `raw-img/cane` topic from the original dataset at
https://www.kaggle.com/datasets/alessiocorrado99/animals10 . The archive SHA-256,
source-file hashes, sizes and download provenance are in `Results/Animals10`.
All 4,863 images are unmodified; the selected topic contains 60,686,716 bytes.
The dataset itself is not bundled in the submission ZIP.

To reproduce after downloading and extracting the dataset:

```bash
python3 Tools/benchmark_animals10.py \
  --topics /absolute/path/to/raw-img --topic cane \
  --results Results/Animals10_New --repeats 3
python3 Tools/plot_results.py --results Results/Animals10_New
```

This tool uses the existing benchmark machinery and scales a predeclared six-rate
fault sweep to the number of DATA chunks. Every recovery case uses identical
probabilities. The maximum satisfies approximately `M*p <= 1.5`, making a full-topic
restart experiment tractable. No timing values are generated or estimated.
The programs perform real transfers and all completed outputs are SHA-256 checked.
Both the report and environment metadata explicitly state that these are one-host
loopback runs. They do not establish physical-LAN performance.

`Tools/benchmark.py` also accepts `--probabilities`, `--parallel-probability` and
`--client-timeout`; its default settings remain the earlier synthetic experiment.
The report updater reads `Results/Animals10` by default, including its recorded
probabilities. It updates table/chart data only; review prose, dataset labels and
analysis whenever new measurements are used.

## Required three-computer experiment

1. Obtain/extract the course-provided Animals10 dataset on the server machine. Supply the directory whose immediate subdirectories are the ten topics (not its parent).
2. Copy this project to two other Linux/WSL client machines, install `g++`, Make and Python, and configure SSH key access from the server machine. Record each machine's CPU/RAM/OS and the link type/rate. Open the server TCP port selected by the harness in the lab firewall, or adapt the harness to a fixed permitted port.
3. Use distinct remote client hosts. `--remote-root` is an absolute path present on both clients; the helper starts clients only and does not need the source dataset there.
4. Run on the server machine, replacing the example addresses/users/paths:

```bash
python3 Tools/lan_benchmark.py \
  --topics /absolute/path/to/raw-img --topic cane \
  --server-ip 192.168.1.10 \
  --host-a student@192.168.1.11 --host-b student@192.168.1.12 \
  --remote-root /home/student/FinalSubmission_Group_097_112 \
  --results Results/LAN --repeats 3
python3 Tools/plot_results.py --results Results/LAN
```

The LAN harness runs total client counts 2/4/8 split evenly between the two machines, varies failure probability and enhanced connections, preserves server logs and remote client outputs, and verifies downloaded files against server-side SHA-256. Both remote helpers wait at a GO barrier. Timing is the controller's release-to-DONE interval (includes small SSH-control overhead, excludes hash checking). A failure stops the run; remote temporary files are cleaned when the helper exits normally. This workflow is supplied for your lab run; no remote machines were available to execute it here. Its small local helper self-check is not a substitute for a distributed test.

5. Add the LAN figures and tables to `Report/main.tex`, describe actual hardware, dataset size, failures and repetitions, and compare them against the labelled local baseline. Do not rename loopback results as LAN results. Full-image topics may need smaller probabilities because the trial is per chunk, not per connection.

## Overleaf / report build

Upload the separately provided `Overleaf_Group_097_112.zip` as a new Overleaf project, or upload the **contents** of `Report/`. Select `main.tex` and pdfLaTeX. The report is self-contained: all tables and charts are embedded in `main.tex`. No shell escape or external assets are required. The separately provided `FinalReport_Group_097_112.tex` is the same source and can be uploaded by itself.

Local compilation:

```bash
cd Report
pdflatex -interaction=nonstopmode -halt-on-error main.tex
pdflatex -interaction=nonstopmode -halt-on-error main.tex
cp main.pdf ../FinalReport_Group_097_112.pdf
```

Edit names, roll numbers and contribution lines in the clearly marked group-details block near the top of `main.tex`. After changing Animals10 results, run `python3 Tools/plot_results.py --results Results/Animals10` for the graphs and `python3 Tools/report_tables.py` for the marked AUTO table/chart regions embedded in the report, then recompile. The latter preserves group details and prose. Review the prose, dataset descriptions and chart scales when numbers change. For LAN evidence, extend the test-setup and performance sections while preserving the distinction between environments.

## Scope and attribution

This implementation was prepared specifically for this package with AI assistance, using the uploaded assignment/model design and Group 097/112 design as requirements. The supplied Group 065/083 report is credited as the layout and organizational reference. The revised report keeps its cover arrangement, five main sections, blue headings, shaded tables and compact page structure. Its code and measurements were not reused; implementation descriptions and results correspond to this package. Group members should review, understand, adapt and validate the work and follow their course's rules on attribution and permitted assistance. The cover's proposed roles are not assertions of completed individual work.

This is a trusted-lab TCP demonstration, not an authenticated Internet file service. There is no TLS, user authentication, cryptographic session ownership, persistent crash recovery, directory recursion, or dynamic source snapshotting. CRC32 detects accidental chunk corruption; SHA-256 is an external test check. Source symlinks and subdirectories are skipped. Unsafe topic/file components and destination symlinks/hard links are refused. Do not expose the service to an untrusted network.
