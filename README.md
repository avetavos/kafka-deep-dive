# Kafka Deep Dive

Bilingual (EN/TH) Apache Kafka 4.x course (KafkaJS as the primary TypeScript client).

## Harness

`tools/verify-snippets.mjs` proves lesson code snippets are real: a real
probe project (`tools/probe/`, gitignored) plus `tsc --noEmit` for
TypeScript/KafkaJS fences, `bash -n` for shell fences, a real config-key
reference check for `properties` fences (catches AI-hallucinated Kafka
config keys), and — for a fence that opts in — an actual run against a real
single-broker Kafka 4.3.1 KRaft container.

```sh
npm run verify                                  # tsc + bash -n + properties key-check, collected fences
node tools/verify-snippets.mjs --props          # properties key-check only
node tools/verify-snippets.mjs --all-ts         # baseline: every typescript fence -> tsc, errors/lesson
node tools/verify-snippets.mjs --all-props      # baseline: every properties fence -> key-check, unknowns/lesson
node tools/verify-snippets.mjs --run [module/lesson]   # docker compose up --wait, run every `// @run` ts fence
node tools/verify-snippets.mjs --cli [module/lesson]   # run collected bash fences inside the broker container
node tools/verify-snippets.mjs --down           # docker compose down -v the probe broker
node tools/verify-snippets.mjs --self-test      # harness self-check
node tools/verify-snippets.mjs --refresh-docs   # refetch tools/probe/config-keys.json
```

### Fence convention

A collectible fence's first line is a path comment naming the real file it
represents; the line is kept (inert) in the written file. **The current
corpus has zero fences with a path comment** (verified baseline — every
existing `typescript`/`bash`/`properties`/`java` fence today is a fragment
under this convention; `--all-ts`/`--all-props` exist specifically to still
get real numbers out of that corpus, see below).

- `` ```typescript `` — first line `// src/<name>.ts` (must start `src/`).
  Second line `// @run` marks it end-to-end runnable (see `--run` below).
- `` ```bash `` — first line `# scripts/<name>.sh`.
- `` ```properties `` — first line `# config/<name>.properties`. Second line
  `# section: producer|consumer|broker|topic|streams|connect` restricts the
  key check to that one official config page; otherwise a key must appear on
  ANY of the six.
- `` ```java `` — first line `// src/main/java/<pkg-path>/<Name>.java`.
  Compile-only (`javac`), and only attempted if `javac` is on `PATH` and
  `kafka-clients`/`kafka-streams`/`slf4j-api` jars for 4.3.1 can be fetched
  from Maven Central into `tools/probe/java/lib/` — otherwise skipped with a
  note, never a hard failure (only 2 java fences exist in the corpus today,
  both fragments — no path comment — so this path is currently untested
  against real content and exists for Phase 3/4 authors).
- A first line containing `@expect-error` is a deliberate-error demo and is
  skipped (the lesson prose carries the real broker/client error text).
  Anything else is a skipped fragment. Fences inside a quiz
  `export const ... = [...]` array or a `<SpotTheBug code={`...`}>` prop are
  excluded before fence-scanning starts (ported from `tools/check-parity.mjs`;
  this course has neither yet — free, future-proof guard for Phase 4).

### `// @run` contract

A ts fence that runs against the real broker MUST:

- read brokers from `process.env.KAFKA_BROKERS ?? 'localhost:9092'`
- prefix every topic name with `process.env.TOPIC_PREFIX ?? ''`
- actually finish (produce/consume/assert what it needs, then let the
  process exit) — `--run` gives it 60s per fence and kills it after that,
  printing the exit code and the last 20 lines of output either way

`--run` uses `KAFKA_BROKERS=localhost:19092` (the probe broker's
host-published port); `--cli` bash fences run with `docker compose exec`,
already inside the container's network namespace, so they use
`BOOTSTRAP=localhost:9092` (the broker's internal listener) instead — the
probe's `docker-compose.yml` publishes two listeners for exactly this split.

### The probe broker

`tools/probe/docker-compose.yml` is a single `apache/kafka:4.3.1` KRaft
container (hard rule for this harness: **one** container, host port
`19092`, torn down with `--down` when done — never left running). Five RF=1
env vars are set explicitly:
`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`,
`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`,
`KAFKA_TRANSACTION_STATE_LOG_MIN_ISR`,
`KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR`,
`KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR` — verified against the SAME
image's own `/etc/kafka/docker/server.properties`, this image already
DEFAULTS every one of these to `1` for a combined single-node KRaft
container, so nothing was actually rejected; they're set explicitly anyway
so a future image version that changes that default can't silently
reintroduce the `GroupCoordinatorNotAvailable`/`NotEnoughReplicas` hang a
single broker gets from a `>1` replication factor on an internal topic (the
"RF=1 gotcha" from the deep review). `KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS=classic,consumer,share`
is the one env var that IS a real change from the image default
(`classic,consumer,streams`) — needed to exercise Share Groups (KIP-932).
All six env var names were checked against `tools/probe/config-keys.json`
(broker section) before use; none were rejected by the image.

### Config-key reference (properties fences)

`kafka.apache.org/documentation/` is itself just a client-side redirect
stub (verified: ~20KB of JS, no content) that maps `#brokerconfigs` etc to
the real per-version pages, e.g. `kafka.apache.org/43/configuration/
broker-configs/` for Kafka 4.3. Each real page lists one entry per config as
`<h4><a id=NAME></a>...`, extracted into `tools/probe/config-keys.json`
(cached; `--refresh-docs` refetches) across the six sections
broker/topic/producer/consumer/streams/connect.

### Baseline numbers (current corpus, before any lesson adds a path comment)

Run `node tools/verify-snippets.mjs --all-ts` and `--all-props` for current
counts; see this harness's introduction report for the numbers as of
2026-09-26.

### Known, accepted gaps

- Default (`npm run verify`) mode only checks fences with a real path
  comment — today that's zero, by design (baseline). `--all-ts`/`--all-props`
  exist so this harness is still useful before any lesson adopts the
  convention.
- `--cli`'s lesson filter matches by namespace prefix (`<module>__<lesson>`);
  a bash fence with no path comment is invisible to it (fragments are never
  collected in default mode).
- Java compile-checking is best-effort (see fence convention above) and has
  no real fences to exercise yet.
