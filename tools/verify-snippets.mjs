#!/usr/bin/env node
// Snippet-verification harness for the bilingual Kafka Deep Dive course.
//
// Modeled on ~/Develops/astro-deep-dive/tools/verify-snippets.mjs (same
// path-comment-on-fence-first-line convention, same probe-directory idea,
// same --self-test shape). Reused verbatim from there: the string-scanning
// helpers (parseStringAt/scanBalanced/findExcludedRanges/stripExcluded) that
// keep a naive fence regex from tripping over a ``` sequence hidden inside a
// quiz `export const x = [...]` array or a `<SpotTheBug code={\`...\`}>`
// prop — this course has neither yet (Phase 4 will add SpotTheBug), but the
// guard is free and future-proof. Everything else is new: this course is
// NOT itself Kafka, so there is no in-process "astro check" equivalent —
// proving a snippet is real means (a) `tsc --noEmit` for TypeScript/KafkaJS,
// (b) `bash -n` for shell fences, (c) every key in a `properties` fence
// checked against the real broker/producer/consumer/topic/streams/connect
// config reference (catches AI-hallucinated config keys), and (d) for
// fences that declare themselves runnable (`// @run`), actually running
// them against a real single-broker Kafka 4.3.1 KRaft container.
//
// Fence convention (spec §1/§5): a fence is a real file only if its FIRST
// line is a path comment:
//   ```typescript  -> // src/<name>.ts        (may end .ts, nested dirs ok)
//   ```bash        -> # scripts/<name>.sh
//   ```properties  -> # config/<name>.properties
//   ```java        -> // src/main/java/<...>.java
// A first line containing `@expect-error` is a deliberate-error demo and is
// skipped (the lesson prose carries the real broker/client error text).
// Anything else (including every fence in the CURRENT corpus — baseline is
// zero path comments, see README/report) is a fragment and is skipped in
// default mode; `--all-ts`/`--all-props` sweep every fence of that language
// regardless, for a baseline number.
//
// A `properties` fence's SECOND line may be `# section: producer` (one of
// producer|consumer|broker|topic|streams|connect) to restrict the key check
// to that one official config page; otherwise a key must appear in ANY of
// the six.
//
// A `typescript` fence's SECOND line may be `// @run` to mark it as
// end-to-end runnable against the real broker (see `--run` below). Contract
// such a fence MUST follow (documented in README): read the broker list from
// `process.env.KAFKA_BROKERS ?? 'localhost:9092'` and prefix every topic
// name with `process.env.TOPIC_PREFIX ?? ''`, and actually exit (produce +
// consume what it needs, then let the process end) — this harness gives it
// 60s and kills it after that.
//
// Namespacing: a collected fence is written into
//   ts:         tools/probe/src/lessons/<module>__<lesson>/<name-after-src/>
//   bash:       tools/probe/scripts/lessons/<module>__<lesson>/<name-after-scripts/>
//   properties: tools/probe/config/lessons/<module>__<lesson>/<name-after-config/>
//   java:       tools/probe/java/lessons/<module>__<lesson>/<name-after-src/main/java/>
// (java keeps its full package-relative path under the lesson namespace so
// package declarations still find their directory when compiled alone.)
//
// Modes:
//   node tools/verify-snippets.mjs              tsc + bash -n + properties-key
//                                                check over COLLECTED (path-
//                                                comment) fences (== `npm run verify`)
//   node tools/verify-snippets.mjs --props       properties-key check only
//   node tools/verify-snippets.mjs --all-ts      every typescript/ts fence (path
//                                                comment or not) as its own file
//                                                -> tsc, error count per lesson
//   node tools/verify-snippets.mjs --all-props   every properties fence (path
//                                                comment or not) -> key check,
//                                                unknown-key count per lesson
//   node tools/verify-snippets.mjs --run [module/lesson]
//                                                docker compose up -d --wait, then
//                                                run every collected `// @run` ts
//                                                fence (all, or one lesson's) with
//                                                tsx, 60s timeout each
//   node tools/verify-snippets.mjs --cli [module/lesson]
//                                                run every collected bash fence
//                                                inside the broker container
//   node tools/verify-snippets.mjs --down        docker compose down -v the probe broker
//   node tools/verify-snippets.mjs --self-test   harness self-check (see selfTest())
//   node tools/verify-snippets.mjs --refresh-docs
//                                                refetch config-keys.json from
//                                                kafka.apache.org (cached otherwise)
//
// Config-key reference: kafka.apache.org/documentation/ is itself just a
// client-side redirect stub (verified: 20KB of JS, no content) that maps
// `#brokerconfigs` etc to `kafka.apache.org/<major><minor>/configuration/
// broker-configs/` (verified against the redirect script's own lookup table
// for Kafka 4.3 -> `/43/...`). Each real config page lists one entry per
// config as `<h4><a id=NAME></a>...` (verified against the installed page
// directly) — that's the only extraction this harness needs.

import { readFileSync, writeFileSync, mkdirSync, rmSync, existsSync, globSync } from 'node:fs';
import { spawnSync, spawn } from 'node:child_process';
import path from 'node:path';

const REPO_ROOT = path.resolve(import.meta.dirname, '..');
const PROBE_DIR = path.join(REPO_ROOT, 'tools/probe');
const DOCS_EN = path.join(REPO_ROOT, 'src/content/docs/en');
const CONFIG_KEYS_PATH = path.join(PROBE_DIR, 'config-keys.json');
const COMPOSE_FILE = path.join(PROBE_DIR, 'docker-compose.yml');

const KAFKA_IMAGE = 'apache/kafka:4.3.1'; // pinned — current stable per kafka.apache.org/downloads at spec time
const HOST_PORT = 19092; // this harness's own port; other agents' compose stacks use other ports (per hard rule)

// Pinned probe devDependency versions, recorded here from `npm view <pkg> version`
// on 2026-09-26 (spec §5's "current versions via npm view, record them"):
//   kafkajs                        2.2.4   (unmaintained since 2023-02-27 — the
//                                           course itself now says so; still the
//                                           client most lesson fences use)
//   @confluentinc/kafka-javascript  1.10.1  (actively maintained, librdkafka-backed;
//                                           GA KIP-848 group.protocol=consumer support)
//   typescript                     7.0.2
//   tsx                             4.23.15
//   @types/node                    26.6.3
const PROBE_DEPS = {
  kafkajs: '2.2.4',
  '@confluentinc/kafka-javascript': '1.10.1',
};
const PROBE_DEV_DEPS = {
  typescript: '7.0.2',
  tsx: '4.23.15',
  '@types/node': '26.6.3',
};

// ---------------------------------------------------------------------------
// Fence languages this harness understands, and each one's path-comment
// convention + probe destination. `strip` is the required source-path
// prefix that gets removed before joining under the lesson namespace dir.
// ---------------------------------------------------------------------------

const FENCE_KINDS = {
  typescript: {
    pathRe: /^\/\/ (src\/[\w@.\-[\]()/]+\.ts)(?:\s+\S.*)?$/,
    strip: 'src/',
    nsRoot: 'src/lessons',
  },
  bash: {
    pathRe: /^# (scripts\/[\w@.\-[\]()/]+\.sh)(?:\s+\S.*)?$/,
    strip: 'scripts/',
    nsRoot: 'scripts/lessons',
  },
  properties: {
    pathRe: /^# (config\/[\w@.\-[\]()/]+\.properties)(?:\s+\S.*)?$/,
    strip: 'config/',
    nsRoot: 'config/lessons',
  },
  java: {
    pathRe: /^\/\/ (src\/main\/java\/[\w@.\-[\]()/]+\.java)(?:\s+\S.*)?$/,
    strip: 'src/main/java/',
    nsRoot: 'java/lessons',
  },
};
const FENCE_LANGS = new Set(Object.keys(FENCE_KINDS));
const SECTIONS = ['broker', 'topic', 'producer', 'consumer', 'streams', 'connect'];

// ---------------------------------------------------------------------------
// String/bracket scanning helpers — ported verbatim from
// astro-deep-dive/tools/verify-snippets.mjs (itself ported from this
// project's own tools/check-parity.mjs). Keeps quiz-array template literals
// and `<SpotTheBug code={\`...\`}>` props from confusing the fence regex.
// ---------------------------------------------------------------------------

function parseStringAt(text, i) {
  const quote = text[i];
  let j = i + 1;
  while (j < text.length) {
    const c = text[j];
    if (c === '\\') {
      j += 2;
      continue;
    }
    if (c === quote) {
      j++;
      break;
    }
    j++;
  }
  return { end: j };
}

function scanBalanced(text, start, open, close) {
  let depth = 1;
  let i = start;
  while (i < text.length && depth > 0) {
    const c = text[i];
    if (c === '"' || c === "'" || c === '`') {
      i = parseStringAt(text, i).end;
      continue;
    }
    if (c === open) depth++;
    else if (c === close) depth--;
    i++;
  }
  return i;
}

function findExcludedRanges(src) {
  const ranges = [];
  {
    const re = /export\s+const\s+\w+\s*=\s*\[/g;
    let m;
    while ((m = re.exec(src))) {
      const end = scanBalanced(src, re.lastIndex, '[', ']');
      ranges.push([m.index, end]);
      re.lastIndex = end;
    }
  }
  {
    const re = /<SpotTheBug\s+code=\{\s*`/g;
    let m;
    while ((m = re.exec(src))) {
      const backtickIdx = m.index + m[0].length - 1;
      const { end } = parseStringAt(src, backtickIdx);
      ranges.push([m.index, end]);
      re.lastIndex = end;
    }
  }
  return ranges;
}

function stripExcluded(src, ranges) {
  if (!ranges.length) return src;
  ranges.sort((a, b) => a[0] - b[0]);
  let out = '';
  let cursor = 0;
  for (const [start, end] of ranges) {
    if (start < cursor) continue;
    out += src.slice(cursor, start);
    out += src.slice(start, end).replace(/[^\n]/g, '');
    cursor = end;
  }
  out += src.slice(cursor);
  return out;
}

function countNewlinesBefore(s, upto) {
  let n = 0;
  for (let i = 0; i < upto; i++) if (s.charCodeAt(i) === 10) n++;
  return n;
}

// Collect every fenced code block in one MDX file's source.
// Returns [{ fenceNum, lang, line, category, path?, body?, section?, run? }]
function collectFences(rawSrc) {
  const src = stripExcluded(rawSrc, findExcludedRanges(rawSrc));
  const fenceRe = /```([\w-]*)[^\n]*\n([\s\S]*?)```/g;
  const results = [];
  let fenceNum = 0;
  let m;
  while ((m = fenceRe.exec(src))) {
    fenceNum++;
    const lang = m[1];
    if (!FENCE_LANGS.has(lang)) continue;
    const body = m[2];
    const line = countNewlinesBefore(src, m.index) + 1;
    const lines = body.split('\n');
    const firstLine = lines[0].trim();
    const secondLine = (lines[1] ?? '').trim();

    if (firstLine.includes('@expect-error')) {
      results.push({ fenceNum, lang, line, category: 'expect-error' });
      continue;
    }
    const pm = FENCE_KINDS[lang].pathRe.exec(firstLine);
    if (!pm) {
      // body kept even though there's no path comment — --all-ts/--all-props
      // need the raw fragment text for their baseline sweep of the CURRENT
      // corpus, which has zero path comments (see README "Fence convention").
      results.push({ fenceNum, lang, line, category: 'skipped-no-path', body });
      continue;
    }
    const entry = { fenceNum, lang, line, category: 'collected', path: pm[1], body };
    if (lang === 'typescript' && secondLine === '// @run') entry.run = true;
    if (lang === 'properties') {
      const sm = /^#\s*section:\s*(\w+)$/.exec(secondLine);
      if (sm && SECTIONS.includes(sm[1])) entry.section = sm[1];
    }
    results.push(entry);
  }
  return results;
}

function destRelPath(ns, lang, srcPath) {
  const { strip, nsRoot } = FENCE_KINDS[lang];
  return path.posix.join(nsRoot, ns, srcPath.slice(strip.length));
}

// ---------------------------------------------------------------------------
// Lesson discovery
// ---------------------------------------------------------------------------

function discoverLessons() {
  const rels = globSync('**/*.mdx', { cwd: DOCS_EN }).sort();
  return rels.map((rel) => {
    const posixRel = rel.replaceAll('\\', '/');
    return {
      absPath: path.join(DOCS_EN, rel),
      mdxRelPath: `src/content/docs/en/${posixRel}`,
      module: posixRel.split('/')[0],
      lesson: path.basename(posixRel, '.mdx'),
    };
  });
}

// ---------------------------------------------------------------------------
// Probe lifecycle
// ---------------------------------------------------------------------------

const TSCONFIG_SRC = {
  compilerOptions: {
    target: 'ES2022',
    module: 'NodeNext',
    moduleResolution: 'NodeNext',
    strict: true,
    esModuleInterop: true,
    skipLibCheck: true,
    noEmit: true,
    types: ['node'],
    pretty: false,
  },
  include: ['src/lessons/**/*.ts', 'src/all-ts/**/*.ts'],
};

function composeYaml() {
  return `# Generated by tools/verify-snippets.mjs — safe to regenerate, do not hand-edit.
# Single-broker Kafka 4.3.1 KRaft container ("the probe's own broker" — hard
# rule: ONE container, torn down with \`--down\` / \`docker compose down -v\`
# when this harness is done; other agents on this host use other ports).
#
# Two listeners on purpose: PLAINTEXT (internal, default port 9092 — used by
# \`--cli\`, which runs bash fences with \`docker compose exec\`, i.e. already
# inside the container's own network namespace) and PLAINTEXT_HOST (the one
# listener actually published to the host, port ${HOST_PORT} — used by \`--run\`,
# which runs ts fences with tsx from the HOST). The default advertised
# listener the image ships (\`PLAINTEXT://localhost:9092\`) is left as-is
# (verified: \`docker run --entrypoint sh apache/kafka:4.3.1 -c 'cat
# /etc/kafka/docker/server.properties'\`) so PLAINTEXT_HOST is the only new
# listener this file adds.
#
# RF=1 env vars: verified against the SAME image's own
# /etc/kafka/docker/server.properties — apache/kafka:4.3.1 already DEFAULTS
# every one of these to 1 for a combined single-node KRaft container. Set
# here explicitly anyway (the deep review's "RF=1 gotcha" — a broker image
# that changes this default would otherwise silently reintroduce
# GroupCoordinatorNotAvailable/NotEnoughReplicas hangs on a single broker).
# All six env var names below were verified against the real config
# reference pages (tools/probe/config-keys.json, broker section) before use;
# none were rejected by this image.
#
# KAFKA_NODE_ID/KAFKA_PROCESS_ROLES/KAFKA_CONTROLLER_QUORUM_VOTERS/
# KAFKA_CONTROLLER_LISTENER_NAMES/KAFKA_INTER_BROKER_LISTENER_NAME: the
# static server.properties baked into the image already has all five of
# these as literal defaults (broker,controller / node 1 / etc — confirmed by
# \`docker run --entrypoint sh apache/kafka:4.3.1 -c 'cat
# /etc/kafka/docker/server.properties'\`), but they're REQUIRED here anyway:
# verified live that as soon as this compose file sets even one KAFKA_*
# env var (KAFKA_LISTENERS, to add the host-published listener below), the
# image's KafkaDockerWrapper switches to fully env-driven config generation
# and no longer falls back to the static file's process.roles — a bare
# \`docker run\` with zero env vars works precisely because it never trips
# that switch. Omitting these five here reproduces exactly the failure this
# note describes: "Missing required configuration process.roles" at
# StorageTool startup (verified via \`docker logs\`, real container, real
# error) — kept in the compose file as the concrete, reproducible reason.
services:
  broker:
    image: ${KAFKA_IMAGE}
    container_name: kafka-deep-dive-probe-broker
    ports:
      - '${HOST_PORT}:${HOST_PORT}'
    environment:
      KAFKA_NODE_ID: '1'
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_LISTENERS: PLAINTEXT://:9092,PLAINTEXT_HOST://:${HOST_PORT},CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_HOST://localhost:${HOST_PORT}
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1'
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1'
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1'
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR: '1'
      KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR: '1'
      KAFKA_GROUP_COORDINATOR_REBALANCE_PROTOCOLS: classic,consumer,share
    healthcheck:
      test: ['CMD-SHELL', '/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092']
      interval: 5s
      timeout: 5s
      retries: 24
      start_period: 15s
`;
}

function ensureProbe() {
  mkdirSync(PROBE_DIR, { recursive: true });
  if (!existsSync(path.join(PROBE_DIR, 'package.json'))) {
    writeFileSync(
      path.join(PROBE_DIR, 'package.json'),
      `${JSON.stringify(
        { name: 'kafka-deep-dive-probe', private: true, type: 'module', dependencies: PROBE_DEPS, devDependencies: PROBE_DEV_DEPS },
        null,
        2,
      )}\n`,
    );
  }
  writeFileSync(path.join(PROBE_DIR, 'tsconfig.json'), `${JSON.stringify(TSCONFIG_SRC, null, 2)}\n`);
  writeFileSync(COMPOSE_FILE, composeYaml());
  if (!existsSync(path.join(PROBE_DIR, 'node_modules'))) {
    console.log('tools/probe/node_modules missing — running npm install...');
    const res = spawnSync('npm', ['install'], { cwd: PROBE_DIR, stdio: 'inherit' });
    if (res.status !== 0) {
      console.error('probe npm install failed');
      process.exit(1);
    }
  }
}

// ---------------------------------------------------------------------------
// Config-key reference (properties fence checker)
// ---------------------------------------------------------------------------

const SECTION_URL = {
  broker: 'https://kafka.apache.org/43/configuration/broker-configs/',
  topic: 'https://kafka.apache.org/43/configuration/topic-configs/',
  producer: 'https://kafka.apache.org/43/configuration/producer-configs/',
  consumer: 'https://kafka.apache.org/43/configuration/consumer-configs/',
  streams: 'https://kafka.apache.org/43/configuration/kafka-streams-configs/',
  connect: 'https://kafka.apache.org/43/configuration/kafka-connect-configs/',
};

// Each real config page lists one entry per config as
// `<h4><a id=NAME></a>...` — verified directly against the installed page
// (kafka.apache.org/documentation/ itself is a client-side redirect stub
// with no content; see file-header note for the real /43/... URLs).
const CONFIG_ID_RE = /<h4><a id=([\w.\-]+)><\/a>/g;

async function fetchConfigKeys() {
  console.log('fetching config key reference from kafka.apache.org/43/configuration/...');
  const sections = {};
  for (const [name, url] of Object.entries(SECTION_URL)) {
    const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
    if (!res.ok) throw new Error(`fetch ${url} failed: ${res.status}`);
    const html = await res.text();
    const keys = new Set();
    let m;
    CONFIG_ID_RE.lastIndex = 0;
    while ((m = CONFIG_ID_RE.exec(html))) keys.add(m[1]);
    if (!keys.size) throw new Error(`no config keys parsed from ${url} — page structure may have changed`);
    sections[name] = [...keys].sort();
    console.log(`  ${name}: ${keys.size} keys`);
  }
  const doc = { fetchedAt: new Date().toISOString(), source: 'https://kafka.apache.org/43/configuration/', sections };
  writeFileSync(CONFIG_KEYS_PATH, `${JSON.stringify(doc, null, 2)}\n`);
  return doc;
}

async function loadConfigKeys(refresh) {
  if (!refresh && existsSync(CONFIG_KEYS_PATH)) {
    return JSON.parse(readFileSync(CONFIG_KEYS_PATH, 'utf8'));
  }
  return fetchConfigKeys();
}

function checkPropertiesBody(body, section, configDoc) {
  const candidateSections = section ? [section] : SECTIONS;
  const allowed = new Set(candidateSections.flatMap((s) => configDoc.sections[s] ?? []));
  const problems = [];
  for (const rawLine of body.split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    const key = line.slice(0, eq).trim();
    if (!allowed.has(key)) problems.push(key);
  }
  return problems;
}

// ---------------------------------------------------------------------------
// buildTrees: collect fences from descriptors and write the probe's trees.
// mode 'default' -> only path-comment 'collected' fences.
// mode 'all-ts' / 'all-props' -> every fence of that one language, path
// comment or not (baseline sweep; synthetic per-fence file name).
// ---------------------------------------------------------------------------

function buildTrees(descriptors, mode = 'default') {
  for (const root of ['src/lessons', 'scripts/lessons', 'config/lessons', 'java/lessons', 'src/all-ts', 'config/all-props']) {
    rmSync(path.join(PROBE_DIR, root), { recursive: true, force: true });
  }
  mkdirSync(path.join(PROBE_DIR, 'src/lessons'), { recursive: true });

  const fenceMap = new Map(); // namespace -> { mdxRelPath, module, lesson, fences: Map(destRelPath -> fenceNum) }
  const stats = new Map(); // module -> { collected, skippedNoPath, expectError }
  const allTsFiles = []; // { relPath, label }
  const allPropsFences = []; // { label, body, section }
  const runFences = []; // { namespace, mdxRelPath, fenceNum, destAbs, module, lesson }
  const bashFences = []; // { namespace, mdxRelPath, fenceNum, destAbs }

  for (const d of descriptors) {
    const counters = stats.get(d.module) ?? { collected: 0, skippedNoPath: 0, expectError: 0 };
    stats.set(d.module, counters);
    const namespace = `${d.module}__${d.lesson}`;
    const nsFences = new Map();
    const src = readFileSync(d.absPath, 'utf8');

    for (const f of collectFences(src)) {
      if (f.category === 'skipped-no-path') counters.skippedNoPath++;
      else if (f.category === 'expect-error') counters.expectError++;
      else counters.collected++;

      if (mode === 'all-ts' && f.lang === 'typescript' && f.category !== 'expect-error' && f.body !== undefined) {
        const rel = `src/all-ts/${namespace}__fence${f.fenceNum}.ts`;
        mkdirSync(path.dirname(path.join(PROBE_DIR, rel)), { recursive: true });
        writeFileSync(path.join(PROBE_DIR, rel), f.body);
        allTsFiles.push({ relPath: rel, module: d.module, lesson: d.lesson, mdxRelPath: d.mdxRelPath, fenceNum: f.fenceNum });
      }
      if (mode === 'all-props' && f.lang === 'properties' && f.category !== 'expect-error' && f.body !== undefined) {
        allPropsFences.push({
          module: d.module,
          lesson: d.lesson,
          mdxRelPath: d.mdxRelPath,
          fenceNum: f.fenceNum,
          body: f.body,
          section: f.section,
        });
      }
      if (mode === 'default' && f.category === 'collected') {
        const dest = destRelPath(namespace, f.lang, f.path);
        nsFences.set(dest, f.fenceNum);
        const destAbs = path.join(PROBE_DIR, dest);
        mkdirSync(path.dirname(destAbs), { recursive: true });
        writeFileSync(destAbs, f.body);
        if (f.lang === 'typescript' && f.run) {
          runFences.push({ namespace, mdxRelPath: d.mdxRelPath, fenceNum: f.fenceNum, destAbs, module: d.module, lesson: d.lesson });
        }
        if (f.lang === 'bash') {
          bashFences.push({ namespace, mdxRelPath: d.mdxRelPath, fenceNum: f.fenceNum, destAbs });
        }
      }
    }
    fenceMap.set(namespace, { mdxRelPath: d.mdxRelPath, module: d.module, lesson: d.lesson, fences: nsFences });
  }

  return { fenceMap, stats, allTsFiles, allPropsFences, runFences, bashFences };
}

function printStats(stats) {
  console.log('\nPer-module fence summary (collected / skipped-no-path / expect-error):');
  const totals = { collected: 0, skippedNoPath: 0, expectError: 0 };
  for (const [module, c] of [...stats.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    console.log(`  ${module}: ${c.collected} / ${c.skippedNoPath} / ${c.expectError}`);
    for (const k of Object.keys(totals)) totals[k] += c[k];
  }
  console.log(`  TOTAL: ${totals.collected} / ${totals.skippedNoPath} / ${totals.expectError}`);
}

// ---------------------------------------------------------------------------
// tsc runner (shared by default-mode ts check and --all-ts)
// ---------------------------------------------------------------------------

const TSC_BIN = path.join(PROBE_DIR, 'node_modules/.bin/tsc');
// tsc, run with --pretty false, prints one diagnostic per line:
//   <relative-path>(<line>,<col>): error TS<code>: <message>
const TSC_DIAG_RE = /^(.+?)\((\d+),(\d+)\): (error|warning) (TS\d+): (.+)$/gm;

function runTsc(tsconfigPath) {
  const res = spawnSync(TSC_BIN, ['-p', tsconfigPath, '--pretty', 'false'], { cwd: PROBE_DIR, encoding: 'utf8' });
  const out = `${res.stdout ?? ''}${res.stderr ?? ''}`;
  const diagnostics = [];
  let m;
  TSC_DIAG_RE.lastIndex = 0;
  while ((m = TSC_DIAG_RE.exec(out))) {
    const [, file, line, col, severity, code, message] = m;
    diagnostics.push({ file: file.replaceAll('\\', '/'), line, col, severity, code, message });
  }
  return { diagnostics, raw: out, status: res.status };
}

function mapLessonFile(fenceMap, relPath, nsRootRe) {
  const lm = nsRootRe.exec(relPath);
  if (!lm) return null;
  const info = fenceMap.get(lm[1]);
  if (!info) return null;
  return { mdxRelPath: info.mdxRelPath, relPath, fenceNum: info.fences.get(relPath) };
}

// ---------------------------------------------------------------------------
// Default mode: tsc (collected ts) + bash -n (collected bash) + properties
// key check (collected properties)
// ---------------------------------------------------------------------------

async function verifyMode({ propsOnly = false } = {}) {
  const { fenceMap, stats, bashFences } = buildTrees(discoverLessons(), 'default');
  const configDoc = await loadConfigKeys(false);

  let tsErrors = 0;
  if (!propsOnly) {
    const { diagnostics } = runTsc(path.join(PROBE_DIR, 'tsconfig.json'));
    const errors = diagnostics.filter((d) => d.severity === 'error');
    tsErrors = errors.length;
    console.log(`\ntsc: ${errors.length} error(s) over collected typescript fences`);
    for (const d of errors) {
      const mapped = mapLessonFile(fenceMap, d.file, /^src\/lessons\/([^/]+)\//);
      const where = mapped ? `${mapped.mdxRelPath}:fence #${mapped.fenceNum}` : `[unmapped] ${d.file}`;
      console.log(`  ${where} — probe:${d.file}:${d.line}:${d.col} ${d.code}: ${d.message}`);
    }
  }

  let bashErrors = 0;
  if (!propsOnly) {
    for (const f of bashFences) {
      const res = spawnSync('bash', ['-n', f.destAbs], { encoding: 'utf8' });
      if (res.status !== 0) {
        bashErrors++;
        console.log(`\nbash -n FAIL ${f.mdxRelPath}:fence #${f.fenceNum} — ${(res.stderr ?? '').trim()}`);
      }
    }
    console.log(`bash -n: ${bashErrors} error(s) over collected bash fences`);
  }

  let propsErrors = 0;
  const descriptors = discoverLessons();
  for (const d of descriptors) {
    const src = readFileSync(d.absPath, 'utf8');
    for (const f of collectFences(src)) {
      if (f.category !== 'collected' || f.lang !== 'properties') continue;
      const problems = checkPropertiesBody(f.body, f.section, configDoc);
      if (problems.length) {
        propsErrors += problems.length;
        console.log(`\nproperties FAIL ${d.mdxRelPath}:fence #${f.fenceNum} — unknown key(s): ${problems.join(', ')}`);
      }
    }
  }
  console.log(`properties: ${propsErrors} unknown key(s) over collected properties fences`);

  printStats(stats);
  const fail = tsErrors > 0 || bashErrors > 0 || propsErrors > 0;
  console.log(fail ? '\nFAIL' : '\nPASS');
  return fail;
}

// ---------------------------------------------------------------------------
// --all-ts / --all-props: baseline sweep over every fence of one language,
// path comment or not (today's corpus has NONE with a path comment).
// ---------------------------------------------------------------------------

function allTsMode() {
  const { allTsFiles } = buildTrees(discoverLessons(), 'all-ts');
  const tsconfig = { ...TSCONFIG_SRC, include: ['src/all-ts/**/*.ts'] };
  const tsconfigPath = path.join(PROBE_DIR, 'tsconfig.all-ts.json');
  writeFileSync(tsconfigPath, `${JSON.stringify(tsconfig, null, 2)}\n`);
  const { diagnostics } = runTsc(tsconfigPath);
  const errors = diagnostics.filter((d) => d.severity === 'error');

  const byLesson = new Map(); // "module/lesson" -> count
  for (const f of allTsFiles) byLesson.set(`${f.module}/${f.lesson}`, (byLesson.get(`${f.module}/${f.lesson}`) ?? 0) + 0);
  const byFile = new Map(allTsFiles.map((f) => [path.join(PROBE_DIR, f.relPath).replaceAll('\\', '/'), f]));
  const errCounts = new Map();
  for (const d of errors) {
    const abs = path.join(PROBE_DIR, d.file).replaceAll('\\', '/');
    const f = byFile.get(abs) ?? [...byFile.values()].find((v) => abs.endsWith(v.relPath));
    const label = f ? `${f.module}/${f.lesson}` : '[unmapped]';
    errCounts.set(label, (errCounts.get(label) ?? 0) + 1);
  }

  console.log(`\n--all-ts: ${allTsFiles.length} typescript fence(s) checked across the corpus, ${errors.length} tsc error(s) total.`);
  console.log('Errors per lesson:');
  for (const [label, count] of [...errCounts.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    console.log(`  ${label}: ${count}`);
  }
  if (!errCounts.size) console.log('  (none)');
  return { total: allTsFiles.length, errors: errors.length, byLesson: errCounts };
}

async function allPropsMode() {
  const { allPropsFences } = buildTrees(discoverLessons(), 'all-props');
  const configDoc = await loadConfigKeys(false);
  const byLesson = new Map();
  let totalUnknown = 0;
  let totalKeys = 0;
  for (const f of allPropsFences) {
    const problems = checkPropertiesBody(f.body, f.section, configDoc);
    const keyCount = f.body.split('\n').filter((l) => l.trim() && !l.trim().startsWith('#') && l.includes('=')).length;
    totalKeys += keyCount;
    totalUnknown += problems.length;
    if (problems.length) {
      const label = `${f.module}/${f.lesson}`;
      byLesson.set(label, (byLesson.get(label) ?? 0) + problems.length);
      console.log(`  ${f.mdxRelPath}:fence #${f.fenceNum} — unknown key(s): ${problems.join(', ')}`);
    }
  }
  console.log(
    `\n--all-props: ${allPropsFences.length} properties fence(s) / ${totalKeys} key(s) checked across the corpus, ${totalUnknown} unknown key(s) total.`,
  );
  console.log('Unknown-key counts per lesson:');
  for (const [label, count] of [...byLesson.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    console.log(`  ${label}: ${count}`);
  }
  if (!byLesson.size) console.log('  (none)');
  return { total: allPropsFences.length, totalKeys, unknown: totalUnknown, byLesson };
}

// ---------------------------------------------------------------------------
// docker compose lifecycle: --run, --cli, --down
// ---------------------------------------------------------------------------

function composeArgs(...rest) {
  return ['compose', '-f', COMPOSE_FILE, '--project-directory', PROBE_DIR, ...rest];
}

function dockerUp() {
  console.log(`starting probe broker (${KAFKA_IMAGE}) on host port ${HOST_PORT}...`);
  const res = spawnSync('docker', composeArgs('up', '-d', '--wait'), { stdio: 'inherit' });
  if (res.status !== 0) {
    console.error('docker compose up --wait failed (broker did not become healthy)');
    process.exit(1);
  }
}

function dockerDown() {
  console.log('tearing down probe broker (docker compose down -v)...');
  spawnSync('docker', composeArgs('down', '-v'), { stdio: 'inherit' });
}

function runOneTsFile(absPath, env, timeoutMs) {
  return new Promise((resolve) => {
    const tsxBin = path.join(PROBE_DIR, 'node_modules/.bin/tsx');
    const child = spawn(tsxBin, [absPath], { cwd: PROBE_DIR, env: { ...process.env, ...env } });
    let out = '';
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill('SIGKILL');
    }, timeoutMs);
    child.stdout.on('data', (d) => (out += d));
    child.stderr.on('data', (d) => (out += d));
    child.on('close', (code) => {
      clearTimeout(timer);
      resolve({ code: timedOut ? null : code, timedOut, output: out });
    });
  });
}

async function runMode(target) {
  dockerUp();
  const { runFences } = buildTrees(discoverLessons(), 'default');
  const filtered = target ? runFences.filter((f) => `${f.module}/${f.lesson}` === target) : runFences;

  if (!filtered.length) {
    console.log(target ? `no // @run fence(s) found for ${target}` : 'no // @run fence(s) found in the corpus');
    return true;
  }

  let anyFail = false;
  for (const f of filtered) {
    const topicPrefix = `${f.module}-${f.lesson}-`;
    console.log(`\nrunning ${f.mdxRelPath}:fence #${f.fenceNum} (KAFKA_BROKERS=localhost:${HOST_PORT}, TOPIC_PREFIX=${topicPrefix})...`);
    const { code, timedOut, output } = await runOneTsFile(
      f.destAbs,
      { KAFKA_BROKERS: `localhost:${HOST_PORT}`, TOPIC_PREFIX: topicPrefix },
      60_000,
    );
    if (timedOut || code !== 0) anyFail = true;
    console.log(`exit code: ${timedOut ? 'TIMEOUT (60s)' : code}`);
    console.log('last 20 lines:');
    console.log(
      output
        .split('\n')
        .slice(-20)
        .map((l) => `  ${l}`)
        .join('\n'),
    );
  }
  return anyFail;
}

function cliMode(target) {
  dockerUp();
  const { bashFences } = buildTrees(discoverLessons(), 'default');
  const filtered = target ? bashFences.filter((f) => f.namespace === target.replace('/', '__')) : bashFences;

  if (!filtered.length) {
    console.log(target ? `no bash fence(s) found for ${target}` : 'no bash fence(s) found in the corpus');
    return true;
  }

  let anyFail = false;
  for (const f of filtered) {
    console.log(`\nrunning ${f.mdxRelPath}:fence #${f.fenceNum} inside the broker container (BOOTSTRAP=localhost:9092)...`);
    const script = readFileSync(f.destAbs, 'utf8');
    const res = spawnSync(
      'docker',
      composeArgs('exec', '-T', '-e', 'BOOTSTRAP=localhost:9092', 'broker', 'bash', '-c', script),
      { encoding: 'utf8' },
    );
    const output = `${res.stdout ?? ''}${res.stderr ?? ''}`;
    if (res.status !== 0) anyFail = true;
    console.log(`exit code: ${res.status}`);
    console.log(
      output
        .split('\n')
        .slice(-20)
        .map((l) => `  ${l}`)
        .join('\n'),
    );
  }
  return anyFail;
}

// ---------------------------------------------------------------------------
// --self-test
// ---------------------------------------------------------------------------

function selfTestMdx() {
  return [
    '---',
    'title: selftest',
    '---',
    '',
    '```typescript',
    '// src/good-producer.ts',
    '// @run',
    "import { Kafka } from 'kafkajs';",
    '',
    "const brokers = [process.env.KAFKA_BROKERS ?? 'localhost:9092'];",
    "const prefix = process.env.TOPIC_PREFIX ?? '';",
    "const topic = `${prefix}selftest`;",
    '',
    'async function main() {',
    "  const kafka = new Kafka({ clientId: 'selftest', brokers });",
    '  const admin = kafka.admin();',
    '  await admin.connect();',
    '  await admin.createTopics({ topics: [{ topic, numPartitions: 1 }] });',
    '  await admin.disconnect();',
    '',
    '  const producer = kafka.producer();',
    '  await producer.connect();',
    "  await producer.send({ topic, messages: [{ value: 'hello' }] });",
    '  await producer.disconnect();',
    '',
    '  const consumer = kafka.consumer({ groupId: `selftest-${Date.now()}` });',
    '  await consumer.connect();',
    '  await consumer.subscribe({ topic, fromBeginning: true });',
    '  await new Promise<void>((resolve) => {',
    '    consumer.run({',
    '      eachMessage: async ({ message }) => {',
    "        console.log('received:', message.value?.toString());",
    '        resolve();',
    '      },',
    '    });',
    '  });',
    '  await consumer.disconnect();',
    '  process.exit(0);',
    '}',
    '',
    'main();',
    '```',
    '',
    '```typescript',
    '// src/bad.ts',
    'const count: number = "oops"; // real type error',
    '```',
    '',
    '```typescript',
    '// @expect-error deliberately fenced, not a real .ts file',
    'throw new Error("boom");',
    '```',
    '',
    '```properties',
    '# config/good.properties',
    '# section: broker',
    'num.io.threads=8',
    '```',
    '',
    '```properties',
    '# config/hallucinated.properties',
    'acks.mode=strong',
    '```',
    '',
    '```bash',
    '# scripts/list-topics.sh',
    'kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list',
    '```',
    '',
  ].join('\n');
}

async function selfTest() {
  ensureProbe();
  const configDoc = await loadConfigKeys(false);

  const descriptors = [
    {
      absPath: null,
      mdxRelPath: 'selftest/self.mdx',
      module: '__selftest__',
      lesson: 'self',
      _src: selfTestMdx(),
    },
  ];
  // discoverLessons()/buildTrees() read from disk via absPath; give this one
  // fence set a real temp file so the rest of the pipeline needs no special-casing.
  const tmpMdx = path.join(PROBE_DIR, '.selftest.mdx');
  writeFileSync(tmpMdx, descriptors[0]._src);
  descriptors[0].absPath = tmpMdx;

  const { fenceMap, runFences, bashFences } = buildTrees(descriptors, 'default');
  rmSync(tmpMdx, { force: true });

  const { diagnostics } = runTsc(path.join(PROBE_DIR, 'tsconfig.json'));
  const errors = diagnostics.filter((d) => d.severity === 'error');
  const ns = '__selftest____self';
  const badTypeFailed = errors.some((d) => mapLessonFile(fenceMap, d.file, /^src\/lessons\/([^/]+)\//)?.relPath === `src/lessons/${ns}/bad.ts`);
  const goodTypePassed = !errors.some(
    (d) => mapLessonFile(fenceMap, d.file, /^src\/lessons\/([^/]+)\//)?.relPath === `src/lessons/${ns}/good-producer.ts`,
  );

  const goodProps = checkPropertiesBody('num.io.threads=8', 'broker', configDoc);
  const badProps = checkPropertiesBody('acks.mode=strong', undefined, configDoc);
  const goodPropsPassed = goodProps.length === 0;
  const badPropsFailed = badProps.length === 1 && badProps[0] === 'acks.mode';

  const bashOk = bashFences.length === 1 && spawnSync('bash', ['-n', bashFences[0].destAbs]).status === 0;

  let runResult = 'skipped (docker not available)';
  let runOk = true; // vacuously true when skipped, per spec
  const dockerCheck = spawnSync('docker', ['info'], { stdio: 'ignore' });
  if (dockerCheck.status === 0) {
    dockerUp();
    const { code, timedOut, output } = await runOneTsFile(
      runFences[0].destAbs,
      { KAFKA_BROKERS: `localhost:${HOST_PORT}`, TOPIC_PREFIX: 'selftest-' },
      60_000,
    );
    runOk = !timedOut && code === 0;
    runResult = runOk ? 'PASS' : `FAIL (exit ${timedOut ? 'TIMEOUT' : code}): ${output.split('\n').slice(-10).join('\n')}`;
    dockerDown();
  }

  const detail = { badTypeFailed, goodTypePassed, goodPropsPassed, badPropsFailed, bashOk, runResult };
  const ok = badTypeFailed && goodTypePassed && goodPropsPassed && badPropsFailed && bashOk && runOk;
  console.log(ok ? '\nself-test: PASS' : '\nself-test: FAIL', detail);
  process.exit(ok ? 0 : 1);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main() {
  const args = process.argv.slice(2);
  ensureProbe();

  if (args.includes('--refresh-docs')) {
    await fetchConfigKeys();
    if (args.length === 1) return;
  }
  if (args.includes('--self-test')) return selfTest();
  if (args.includes('--down')) return dockerDown();

  const runIdx = args.indexOf('--run');
  if (runIdx !== -1) {
    const fail = await runMode(args[runIdx + 1]);
    process.exit(fail ? 1 : 0);
  }
  const cliIdx = args.indexOf('--cli');
  if (cliIdx !== -1) {
    const fail = cliMode(args[cliIdx + 1]);
    process.exit(fail ? 1 : 0);
  }
  if (args.includes('--all-ts')) {
    const { errors } = allTsMode();
    process.exit(errors > 0 ? 1 : 0);
  }
  if (args.includes('--all-props')) {
    const { unknown } = await allPropsMode();
    process.exit(unknown > 0 ? 1 : 0);
  }

  const fail = await verifyMode({ propsOnly: args.includes('--props') });
  process.exit(fail ? 1 : 0);
}

main();
