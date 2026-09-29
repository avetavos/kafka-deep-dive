// Pure logic for <TSPlayground>. No DOM access at module scope so this file
// can be unit-tested under plain Node (vitest, default "node" environment).
// DOM wiring lives in TSPlayground.astro's inline <script>.

export type TS = typeof import('typescript');

export const TS_CDN_VERSION = '5.9.3';
const CDN_BASE = `https://cdn.jsdelivr.net/npm/typescript@${TS_CDN_VERSION}/lib/`;

export const CHECK_BADGE = {
  en: `checked with TS ${TS_CDN_VERSION} — the course text targets 7.0`,
  th: `ตรวจด้วย TS ${TS_CDN_VERSION} — เนื้อหาคอร์สอิง 7.0`,
};

// ---------------------------------------------------------------------------
// Lazy-load the TS compiler (UMD build defines the global `ts`) from jsdelivr.
// ---------------------------------------------------------------------------

declare global {
  // eslint-disable-next-line no-var
  var ts: TS | undefined;
}

let tsPromise: Promise<TS> | null = null;

function loadScript(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const s = document.createElement('script');
    s.src = src;
    s.onload = () => resolve();
    s.onerror = () => reject(new Error('failed to load ' + src));
    document.head.appendChild(s);
  });
}

export function loadTypeScript(): Promise<TS> {
  if (globalThis.ts) return Promise.resolve(globalThis.ts);
  if (tsPromise) return tsPromise;
  tsPromise = (async () => {
    await loadScript(`${CDN_BASE}typescript.js`);
    if (!globalThis.ts) throw new Error('typescript.js loaded but did not define `ts`');
    return globalThis.ts;
  })();
  return tsPromise;
}

// ---------------------------------------------------------------------------
// Lib-file fetching (browser: CDN; tests: node_modules/typescript-browser/lib)
// ---------------------------------------------------------------------------

export type LibLoader = (fileName: string) => Promise<string | undefined>;

export function fetchLibFromCdn(fileName: string): Promise<string | undefined> {
  return fetch(`${CDN_BASE}${fileName}`).then((r) => (r.ok ? r.text() : undefined));
}

const REFERENCE_RE = /\/\/\/\s*<reference\s+(lib|path)\s*=\s*"([^"]+)"\s*\/>/g;

export interface FetchedLib {
  name: string;
  bytes: number;
}

/** Recursively resolves `/// <reference lib="…" />` / `path="…"` chains into `cache`. */
async function fetchLibClosure(
  name: string,
  cache: Map<string, string>,
  loadLib: LibLoader,
  fetched: FetchedLib[],
): Promise<void> {
  if (cache.has(name)) return;
  const text = await loadLib(name);
  if (text === undefined) throw new Error(`lib file not found: ${name}`);
  cache.set(name, text);
  fetched.push({ name, bytes: byteLength(text) });

  const refs: string[] = [];
  for (const m of text.matchAll(REFERENCE_RE)) {
    const [, kind, ref] = m;
    refs.push(kind === 'lib' ? `lib.${ref}.d.ts` : ref);
  }
  for (const ref of refs) {
    if (!cache.has(ref)) await fetchLibClosure(ref, cache, loadLib, fetched);
  }
}

function byteLength(s: string): number {
  // Avoid depending on TextEncoder in every environment; UTF-8 byte count.
  return typeof Buffer !== 'undefined' ? Buffer.byteLength(s, 'utf8') : new TextEncoder().encode(s).length;
}

// ---------------------------------------------------------------------------
// Check types — in-memory CompilerHost + Program
// ---------------------------------------------------------------------------

export interface CheckResult {
  diagnostics: string[];
  libFilesFetched: FetchedLib[];
}

const MAIN_FILE = 'input.ts';
let sharedLibCache: Map<string, string> | null = null;

function baseName(fileName: string): string {
  return fileName.replace(/^.*[\\/]/, '');
}

// A same-origin ambient .d.ts (e.g. public/kafkajs-types.d.ts) registered as
// an extra root file alongside input.ts. It only needs a `declare module 'x'`
// body — TypeScript matches ambient module declarations by name wherever they
// appear in the program, so no node_modules path resolution is required.
export interface ExtraLib {
  fileName: string;
  text: string;
}

function createInMemoryHost(
  ts: TS,
  mainText: string,
  libCache: Map<string, string>,
  extraLibs: ExtraLib[] = [],
): import('typescript').CompilerHost {
  const extraMap = new Map(extraLibs.map((l) => [l.fileName, l.text]));
  return {
    getSourceFile(fileName, languageVersion) {
      if (fileName === MAIN_FILE) return ts.createSourceFile(fileName, mainText, languageVersion, true);
      if (extraMap.has(fileName)) return ts.createSourceFile(fileName, extraMap.get(fileName)!, languageVersion, true);
      const text = libCache.get(baseName(fileName));
      return text === undefined ? undefined : ts.createSourceFile(fileName, text, languageVersion, true);
    },
    getDefaultLibFileName: (options) => ts.getDefaultLibFileName(options),
    writeFile: () => {},
    getCurrentDirectory: () => '/',
    getCanonicalFileName: (f) => f,
    useCaseSensitiveFileNames: () => true,
    getNewLine: () => '\n',
    fileExists: (fileName) => fileName === MAIN_FILE || extraMap.has(fileName) || libCache.has(baseName(fileName)),
    readFile: (fileName) =>
      fileName === MAIN_FILE ? mainText : extraMap.get(fileName) ?? libCache.get(baseName(fileName)),
    directoryExists: () => true,
    getDirectories: () => [],
  };
}

export function formatDiagnostics(ts: TS, diagnostics: readonly import('typescript').Diagnostic[]): string[] {
  return diagnostics.map((d) => {
    const message = ts.flattenDiagnosticMessageText(d.messageText, '\n');
    if (d.file && d.start !== undefined) {
      const { line, character } = d.file.getLineAndCharacterOfPosition(d.start);
      return `${line + 1}:${character + 1} TS${d.code}: ${message}`;
    }
    return `TS${d.code}: ${message}`;
  });
}

/**
 * Type-checks `code` with an in-memory CompilerHost. `loadLib` supplies the
 * text of any `lib.*.d.ts` file by name (browser: CDN fetch; tests: read
 * from node_modules/typescript-browser/lib). All lib files reachable via
 * `/// <reference .../>` from the default lib are fetched up front, because
 * our own `getSourceFile` must be synchronous once ts.createProgram runs.
 */
export async function checkTypes(
  ts: TS,
  code: string,
  loadLib: LibLoader,
  extraLibs: ExtraLib[] = [],
): Promise<CheckResult> {
  const options: import('typescript').CompilerOptions = {
    strict: true,
    target: ts.ScriptTarget.ES2024,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    noEmit: true,
    skipLibCheck: true,
  };

  // Lib files never change for a given CDN version, so cache them across
  // calls (module-scoped) rather than refetching ~2.4MB on every click.
  const libCache = sharedLibCache ?? (sharedLibCache = new Map<string, string>());
  const fetched: FetchedLib[] = [];
  const defaultLib = ts.getDefaultLibFileName(options);
  await fetchLibClosure(defaultLib, libCache, loadLib, fetched);

  const host = createInMemoryHost(ts, code, libCache, extraLibs);
  const program = ts.createProgram([MAIN_FILE, ...extraLibs.map((l) => l.fileName)], options, host);
  const diagnostics = ts.getPreEmitDiagnostics(program);

  return { diagnostics: formatDiagnostics(ts, diagnostics), libFilesFetched: fetched };
}

// ---------------------------------------------------------------------------
// Extra ambient lib fetching (same-origin, e.g. public/kafkajs-types.d.ts) —
// separate from fetchLibFromCdn's jsdelivr fetch, cached per URL.
// ---------------------------------------------------------------------------

const extraLibTextCache = new Map<string, Promise<string>>();

export function fetchExtraLibText(url: string): Promise<string> {
  let p = extraLibTextCache.get(url);
  if (!p) {
    p = fetch(url).then((r) => {
      if (!r.ok) throw new Error(`failed to fetch ${url}: ${r.status}`);
      return r.text();
    });
    extraLibTextCache.set(url, p);
  }
  return p;
}

// ---------------------------------------------------------------------------
// Run — transpile + execute in a sandboxed iframe
// ---------------------------------------------------------------------------

/** Detects a top-level `import ...` statement (not a dynamic `import(...)` call). */
export function hasTopLevelImport(code: string): boolean {
  return /^\s*import\s+(?!\()/m.test(code);
}

/** Strips leading `export` (including `export default`) so module-shaped snippets run as a plain script. */
export function stripExportKeywords(code: string): string {
  return code
    .replace(/^(\s*)export\s+default\s+/gm, '$1')
    .replace(/^(\s*)export\s+(?=(const|let|var|function|class|interface|type|enum|async\s+function)\b)/gm, '$1');
}

export function transpileForRun(ts: TS, code: string): string {
  const stripped = stripExportKeywords(code);
  const result = ts.transpileModule(stripped, {
    compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
  });
  return result.outputText;
}

export function buildSandboxSrcdoc(jsCode: string): string {
  const safe = jsCode.replace(/<\/script/gi, '<\\/script');
  return (
    '<!doctype html><html><head><meta charset="utf-8"></head><body><script>' +
    '(function(){' +
    'function post(type,args){try{window.parent.postMessage({__tsPlayground:true,type:type,args:args.map(function(a){try{return typeof a==="object"?JSON.stringify(a):String(a)}catch(e){return String(a)}})},"*")}catch(e){}}' +
    'console.log=function(){post("log",Array.prototype.slice.call(arguments))};' +
    'console.info=console.log;' +
    'console.warn=function(){post("warn",Array.prototype.slice.call(arguments))};' +
    'console.error=function(){post("error",Array.prototype.slice.call(arguments))};' +
    'window.onerror=function(m){post("error",[String(m)]);post("done",[]);return true};' +
    'try{\n' + safe + '\n;post("done",[])' +
    '}catch(e){post("error",[(e&&e.message)||String(e)]);post("done",[])}' +
    '})();' +
    '</script></body></html>'
  );
}

export interface SandboxResult {
  lines: string[];
  errorLines: string[];
  timedOut: boolean;
}

/** Runs `jsCode` inside a sandboxed, hidden iframe and collects console output via postMessage. */
export function runInSandbox(jsCode: string, timeoutMs = 5000): Promise<SandboxResult> {
  return new Promise((resolve) => {
    const lines: string[] = [];
    const errorLines: string[] = [];
    let settled = false;

    const iframe = document.createElement('iframe');
    iframe.setAttribute('sandbox', 'allow-scripts');
    iframe.style.display = 'none';

    const timer = setTimeout(() => finish(true), timeoutMs);

    function onMessage(e: MessageEvent) {
      const data = e.data as { __tsPlayground?: boolean; type?: string; args?: string[] } | undefined;
      if (!data?.__tsPlayground) return;
      if (data.type === 'log' || data.type === 'warn') lines.push((data.args ?? []).join(' '));
      else if (data.type === 'error') errorLines.push((data.args ?? []).join(' '));
      else if (data.type === 'done') finish(false);
    }

    function finish(timedOut: boolean) {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      window.removeEventListener('message', onMessage);
      iframe.remove();
      resolve({ lines, errorLines, timedOut });
    }

    window.addEventListener('message', onMessage);
    iframe.srcdoc = buildSandboxSrcdoc(jsCode);
    document.body.appendChild(iframe);
  });
}

// ---------------------------------------------------------------------------
// lz-string (compressToEncodedURIComponent / decompress) — inlined, no CDN dep.
// Port of pieroxy/lz-string (WTFPL/MIT, public domain-equivalent), trimmed to
// only the EncodedURIComponent variant this component needs.
// ---------------------------------------------------------------------------

const KEY_STR_URI_SAFE = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+-$';

function lzCompress(uncompressed: string, bitsPerChar: number, getCharFromInt: (a: number) => string): string {
  if (uncompressed == null) return '';
  let i: number;
  let value: number;
  const context_dictionary: Record<string, number> = {};
  const context_dictionaryToCreate: Record<string, boolean> = {};
  let context_c = '';
  let context_wc = '';
  let context_w = '';
  let context_enlargeIn = 2;
  let context_dictSize = 3;
  let context_numBits = 2;
  const context_data: string[] = [];
  let context_data_val = 0;
  let context_data_position = 0;

  const emitBit = (bit: number) => {
    context_data_val = (context_data_val << 1) | bit;
    if (context_data_position === bitsPerChar - 1) {
      context_data_position = 0;
      context_data.push(getCharFromInt(context_data_val));
      context_data_val = 0;
    } else {
      context_data_position++;
    }
  };

  const emitChar = (charCode: number, numBits: number) => {
    let v = charCode;
    for (i = 0; i < numBits; i++) {
      emitBit(v & 1);
      v = v >> 1;
    }
  };

  const flushWord = (w: string) => {
    if (Object.prototype.hasOwnProperty.call(context_dictionaryToCreate, w)) {
      if (w.charCodeAt(0) < 256) {
        for (i = 0; i < context_numBits; i++) emitBit(0);
        emitChar(w.charCodeAt(0), 8);
      } else {
        value = 1;
        for (i = 0; i < context_numBits; i++) {
          emitBit(value);
          value = 0;
        }
        emitChar(w.charCodeAt(0), 16);
      }
      context_enlargeIn--;
      if (context_enlargeIn === 0) {
        context_enlargeIn = Math.pow(2, context_numBits);
        context_numBits++;
      }
      delete context_dictionaryToCreate[w];
    } else {
      emitChar(context_dictionary[w], context_numBits);
    }
    context_enlargeIn--;
    if (context_enlargeIn === 0) {
      context_enlargeIn = Math.pow(2, context_numBits);
      context_numBits++;
    }
  };

  for (let ii = 0; ii < uncompressed.length; ii += 1) {
    context_c = uncompressed.charAt(ii);
    if (!Object.prototype.hasOwnProperty.call(context_dictionary, context_c)) {
      context_dictionary[context_c] = context_dictSize++;
      context_dictionaryToCreate[context_c] = true;
    }

    context_wc = context_w + context_c;
    if (Object.prototype.hasOwnProperty.call(context_dictionary, context_wc)) {
      context_w = context_wc;
    } else {
      flushWord(context_w);
      context_dictionary[context_wc] = context_dictSize++;
      context_w = String(context_c);
    }
  }

  if (context_w !== '') flushWord(context_w);

  // Mark end of stream (value 2).
  value = 2;
  for (i = 0; i < context_numBits; i++) {
    emitBit(value & 1);
    value = value >> 1;
  }

  // Flush remaining bits.
  // eslint-disable-next-line no-constant-condition
  while (true) {
    context_data_val = context_data_val << 1;
    if (context_data_position === bitsPerChar - 1) {
      context_data.push(getCharFromInt(context_data_val));
      break;
    } else {
      context_data_position++;
    }
  }
  return context_data.join('');
}

function lzDecompress(length: number, resetValue: number, getNextValue: (index: number) => number): string | null {
  const dictionary: string[] = [];
  let enlargeIn = 4;
  let dictSize = 4;
  let numBits = 3;
  let entry = '';
  const result: string[] = [];
  let i: number;
  let w: string;
  let bits: number;
  let resb: number;
  let maxpower: number;
  let power: number;
  let c: number;

  const data = { val: getNextValue(0), position: resetValue, index: 1 };

  for (i = 0; i < 3; i += 1) dictionary[i] = String(i);

  const readBits = (n: number): number => {
    bits = 0;
    maxpower = Math.pow(2, n);
    power = 1;
    while (power !== maxpower) {
      resb = data.val & data.position;
      data.position >>= 1;
      if (data.position === 0) {
        data.position = resetValue;
        data.val = getNextValue(data.index++);
      }
      bits |= (resb > 0 ? 1 : 0) * power;
      power <<= 1;
    }
    return bits;
  };

  const next = readBits(2);
  let firstChar: string;
  if (next === 0) firstChar = String.fromCharCode(readBits(8));
  else if (next === 1) firstChar = String.fromCharCode(readBits(16));
  else return '';

  dictionary[3] = firstChar;
  w = firstChar;
  result.push(firstChar);

  // eslint-disable-next-line no-constant-condition
  while (true) {
    if (data.index > length) return '';
    c = readBits(numBits);

    if (c === 0) {
      dictionary[dictSize++] = String.fromCharCode(readBits(8));
      c = dictSize - 1;
      enlargeIn--;
    } else if (c === 1) {
      dictionary[dictSize++] = String.fromCharCode(readBits(16));
      c = dictSize - 1;
      enlargeIn--;
    } else if (c === 2) {
      return result.join('');
    }

    if (enlargeIn === 0) {
      enlargeIn = Math.pow(2, numBits);
      numBits++;
    }

    if (dictionary[c]) {
      entry = dictionary[c];
    } else if (c === dictSize) {
      entry = w + w.charAt(0);
    } else {
      return null;
    }
    result.push(entry);
    dictionary[dictSize++] = w + entry.charAt(0);
    enlargeIn--;
    w = entry;
    if (enlargeIn === 0) {
      enlargeIn = Math.pow(2, numBits);
      numBits++;
    }
  }
}

export function compressToEncodedURIComponent(input: string): string {
  if (input == null) return '';
  return lzCompress(input, 6, (a) => KEY_STR_URI_SAFE.charAt(a));
}

export function decompressFromEncodedURIComponent(input: string | null): string | null {
  if (input == null) return '';
  if (input === '') return null;
  const normalized = input.replace(/ /g, '+');
  return lzDecompress(normalized.length, 32, (index) => KEY_STR_URI_SAFE.indexOf(normalized.charAt(index)));
}

export function buildPlaygroundUrl(code: string): string {
  return `https://www.typescriptlang.org/play/#code/${compressToEncodedURIComponent(code)}`;
}
