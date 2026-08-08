import { readFile, writeFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const HERE = dirname(fileURLToPath(import.meta.url));

const ARGS = new Set(process.argv.slice(2));
const DRY_RUN = ARGS.has('--dry-run');
const ONCE = ARGS.has('--once');

const NTFY_URL = process.env.NTFY_URL || 'https://ntfy.sh';
const NTFY_TOPIC = process.env.NTFY_TOPIC || '';
const STATE_FILE = process.env.STATE_FILE || join(HERE, 'state.json');
const SITES_FILE = process.env.SITES_FILE || join(HERE, 'sites.json');

const VERBOSE = process.env.VERBOSE === '1' || DRY_RUN;
const log = (...a) => { if (VERBOSE) console.log(...a); };

const onFatal = async (e) => {
  await ntfy(
    'MONITOR ERROR',
    `Ο prober σταμάτησε: ${String(e?.message || e).slice(0, 200)}`,
    { priority: 4, tags: 'boom' },
  );
  console.error('run failed');
  process.exit(1);
};
process.on('unhandledRejection', onFatal);
process.on('uncaughtException', onFatal);

const FAIL_THRESHOLD = 2;
const RENOTIFY_HOURS = 4;
const STORM_THRESHOLD = 5;
const MAX_BODY_BYTES = 3800;

const ATTEMPTS = ONCE ? 1 : 3;
const ATTEMPT_GAP_MS = 3000;
const TIMEOUT_MS = 20000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function attempt(site) {
  const started = Date.now();
  try {
    const res = await fetch(site.url, {
      redirect: 'follow',
      signal: AbortSignal.timeout(TIMEOUT_MS),
      headers: { 'user-agent': process.env.USER_AGENT || 'uptime-check/1.0' },
    });
    const body = await res.text();
    const ms = Date.now() - started;

    const expect = site.expect ?? 200;
    if (res.status !== expect) {
      return { ok: false, status: res.status, ms, reason: `HTTP ${res.status}, περίμενα ${expect}` };
    }
    if (site.assert && !body.includes(site.assert)) {
      return { ok: false, status: res.status, ms, reason: `λείπει το κείμενο "${site.assert}"` };
    }
    return { ok: true, status: res.status, ms, bytes: body.length };
  } catch (e) {
    const raw = String(e?.cause?.code || e?.message || e);
    return { ok: false, status: 0, ms: Date.now() - started, reason: raw.slice(0, 80) };
  }
}

async function probe(site) {
  let last;
  for (let i = 0; i < ATTEMPTS; i++) {
    last = await attempt(site);
    if (last.ok) return last;
    if (i < ATTEMPTS - 1) await sleep(ATTEMPT_GAP_MS);
  }
  return last;
}

function asciiTitle(s) {
  return s.replace(/[^\x20-\x7E]/g, '?');
}

function clampBytes(s, max) {
  if (Buffer.byteLength(s, 'utf8') <= max) return s;
  let lo = 0;
  let hi = s.length;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    if (Buffer.byteLength(s.slice(0, mid), 'utf8') <= max) lo = mid;
    else hi = mid - 1;
  }
  return s.slice(0, lo);
}

async function ntfy(title, rawBody, { priority = 3, tags = 'bar_chart' } = {}) {
  const clamped = clampBytes(rawBody, MAX_BODY_BYTES);
  const body = clamped === rawBody ? rawBody : `${clamped}\n… (συντομεύτηκε)`;

  if (DRY_RUN) {
    console.log(`--- ntfy [${priority}/${tags}] ${title} (${Buffer.byteLength(body, 'utf8')}B)\n${body}\n`);
    return true;
  }
  if (!NTFY_TOPIC) return false;

  try {
    const res = await fetch(`${NTFY_URL.replace(/\/$/, '')}/${NTFY_TOPIC}`, {
      method: 'POST',
      body,
      headers: {
        Title: asciiTitle(title),
        Priority: String(priority),
        Tags: tags,
        'Content-Type': 'text/plain; charset=utf-8',
      },
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) {
      log(`ntfy failed: HTTP ${res.status} ${await res.text()}`);
      return false;
    }
    return true;
  } catch (e) {
    log(`ntfy failed: ${e?.message || e}`);
    return false;
  }
}

async function loadJson(path, fallback) {
  try {
    return JSON.parse(await readFile(path, 'utf8'));
  } catch {
    return fallback;
  }
}

const minutesSince = (ts) => Math.round((Date.now() - ts) / 60000);

function humanDuration(mins) {
  if (mins < 60) return `${mins} λεπτά`;
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return m ? `${h}ω ${m}λ` : `${h} ώρες`;
}

async function loadRegistry() {
  if (process.env.SITES_JSON) {
    try {
      return JSON.parse(process.env.SITES_JSON);
    } catch {
      console.error('SITES_JSON is not valid JSON');
      process.exit(1);
    }
  }
  return loadJson(SITES_FILE, []);
}

if (!DRY_RUN && !NTFY_TOPIC) {
  console.error('NTFY_TOPIC is not set — no alerts could be delivered');
  process.exit(1);
}

const sites = (await loadRegistry()).filter((s) => s.enabled !== false);
if (!sites.length) {
  console.error('no enabled sites in the registry');
  process.exit(1);
}

const state = await loadJson(STATE_FILE, {});
const results = await Promise.all(sites.map(async (s) => [s, await probe(s)]));

const now = Date.now();
let downCount = 0;

const events = { down: [], stillDown: [], up: [], slow: [] };

for (const [site, result] of results) {
  const key = site.name;
  const prev = state[key] || { fails: 0, downSince: null, notified: 0 };

  if (result.ok) {
    if (prev.downSince) {
      events.up.push({ site, result, mins: minutesSince(prev.downSince) });
    }
    const slowMs = site.slowMs ?? 8000;
    if (result.ms > slowMs) {
      events.slow.push({ site, result, slowMs });
    }
    state[key] = { fails: 0, downSince: null, notified: 0, lastMs: result.ms, lastStatus: result.status };
    continue;
  }

  const fails = prev.fails + 1;
  const isDown = fails >= FAIL_THRESHOLD;
  const downSince = prev.downSince || (isDown ? now : null);

  if (isDown) {
    downCount++;
    const isNew = !prev.downSince;
    const isStale = now - prev.notified >= RENOTIFY_HOURS * 3600 * 1000;

    if (isNew) events.down.push({ site, result, key });
    else if (isStale) events.stillDown.push({ site, result, key, mins: minutesSince(downSince) });
  }

  state[key] = { fails, downSince, notified: prev.notified, lastMs: result.ms, lastStatus: result.status };
}

const stamp = (list) => {
  for (const e of list) if (e.key && state[e.key]) state[e.key].notified = now;
};

if (events.down.length >= STORM_THRESHOLD) {
  const body =
    `Έπεσαν ${events.down.length} sites ταυτόχρονα — πιθανή καθολική βλάβη, ` +
    `όχι ${events.down.length} ξεχωριστά προβλήματα.\n\n` +
    events.down.map((e) => `${e.site.name} — ${e.result.reason}`).join('\n');
  if (await ntfy(`DOWN: ${events.down.length} sites`, body, { priority: 5, tags: 'rotating_light' })) {
    stamp(events.down);
  }
} else {
  for (const e of events.down) {
    const sent = await ntfy(`DOWN: ${e.site.name}`, `${e.site.url}\n${e.result.reason}`, {
      priority: e.site.tier === 'production' ? 5 : 4,
      tags: 'rotating_light',
    });
    if (sent) stamp([e]);
  }
}

if (events.stillDown.length >= STORM_THRESHOLD) {
  const body = events.stillDown
    .map((e) => `${e.site.name} — ${humanDuration(e.mins)}`)
    .join('\n');
  if (await ntfy(`STILL DOWN: ${events.stillDown.length} sites`, body, { priority: 5, tags: 'rotating_light' })) {
    stamp(events.stillDown);
  }
} else {
  for (const e of events.stillDown) {
    const sent = await ntfy(
      `STILL DOWN: ${e.site.name}`,
      `${e.site.url}\n${e.result.reason} · ${humanDuration(e.mins)}`,
      { priority: e.site.tier === 'production' ? 5 : 4, tags: 'rotating_light' },
    );
    if (sent) stamp([e]);
  }
}

if (events.up.length >= STORM_THRESHOLD) {
  const longest = Math.max(...events.up.map((e) => e.mins));
  await ntfy(
    `UP: ${events.up.length} sites`,
    `Επανήλθαν όλα. Μεγαλύτερη διακοπή ${humanDuration(longest)}.\n\n` +
      events.up.map((e) => e.site.name).join('\n'),
    { priority: 3, tags: 'white_check_mark' },
  );
} else {
  for (const e of events.up) {
    await ntfy(
      `UP: ${e.site.name}`,
      `Επανήλθε μετά από ${humanDuration(e.mins)}. Απόκριση ${e.result.ms}ms.`,
      { priority: 3, tags: 'white_check_mark' },
    );
  }
}

if (events.slow.length >= STORM_THRESHOLD) {
  await ntfy(
    `SLOW: ${events.slow.length} sites`,
    events.slow.map((e) => `${e.site.name} — ${e.result.ms}ms`).join('\n'),
    { priority: 2, tags: 'turtle' },
  );
} else {
  for (const e of events.slow) {
    await ntfy(`SLOW: ${e.site.name}`, `Απόκριση ${e.result.ms}ms, πάνω από το όριο των ${e.slowMs}ms.`, {
      priority: 2,
      tags: 'turtle',
    });
  }
}

for (const [site, r] of results.sort((a, b) => a[0].name.localeCompare(b[0].name))) {
  log(
    `${r.ok ? 'ok  ' : 'FAIL'} ${site.name.padEnd(46)} ${String(r.status).padStart(3)} ${String(r.ms).padStart(6)}ms  ${r.ok ? '' : r.reason}`,
  );
}
log(`\n${results.length} sites · ${downCount} down`);

if (!DRY_RUN) {
  await writeFile(STATE_FILE, JSON.stringify(state, null, 2));
}

process.exit(0);
