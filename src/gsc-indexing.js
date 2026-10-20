#!/usr/bin/env node
/**
 * gsc-indexing.js — GSC Indexing API Submission
 *
 * Pipeline (each step is a separate CLI mode, designed to run on a daily cron):
 *   1. Scan: sitemap (+ optional GSC Analytics / crawler / CSV) → URL Inspection API
 *   2. Optionally merges findings from an upstream monitor job (.monitor-state.json)
 *   3. Submits all not-indexed URLs to Google Indexing API (200/day)
 *   4. Rechecks previously submitted URLs to see if they got indexed
 *
 * ─────────────────────────────────────────────────────────────
 * USAGE
 * ─────────────────────────────────────────────────────────────
 *   node gsc-indexing.js --scan            # Light scan: sitemap + monitor merge
 *   node gsc-indexing.js --scan-full       # Deep scan: + GSC Analytics + crawler + CSV (weekly)
 *   node gsc-indexing.js                   # Submit unindexed to Indexing API
 *   node gsc-indexing.js --recheck         # Re-inspect previously submitted
 *   node gsc-indexing.js --status          # Print summary
 *   node gsc-indexing.js --submit-url URL  # Force-submit a specific URL
 *   node gsc-indexing.js --dry-run         # Show what would be submitted, send nothing
 *
 *  To import GSC "Crawled not indexed" URLs:
 *   1. Go to GSC → Pages → Click "Crawled - currently not indexed"
 *   2. Click Export → Download CSV
 *   3. Save as ./gsc-not-indexed.csv
 *   4. Run: node gsc-indexing.js --scan
 *   The CSV URLs will be auto-imported and inspected.
 *
 * ─────────────────────────────────────────────────────────────
 * DAILY LIMITS
 * ─────────────────────────────────────────────────────────────
 *   Indexing API submit:      200 URLs/day
 *   URL Inspection API check: 2,000 URLs/day
 *   Both reset at midnight Pacific time
 */

require('dotenv').config({ override: true });
const fs   = require('fs');
const path = require('path');
const { XMLParser } = require('fast-xml-parser');
const { GoogleAuth } = require('google-auth-library');
const { isIndexablePageUrl, parseInspection, auditPage, diagnose } = require('./gsc-lib');

// ─────────────────────────────────────────────────────────────
// Config
// ─────────────────────────────────────────────────────────────
const SA_PATH    = process.env.GOOGLE_SERVICE_ACCOUNT_PATH || './service-account.json';
const SITE_URL   = (process.env.GSC_SITE_URL || 'https://example.com/').replace(/\/$/, '') + '/';
const STORE      = process.env.SHOPIFY_STORE || 'your-store.myshopify.com';
const BLOG_HANDLE = process.env.SHOPIFY_BLOG_HANDLE || 'news';
const USER_AGENT = process.env.CRAWLER_USER_AGENT || 'SEOIndexingBot/1.0';
const STATE_DIR  = process.env.STATE_DIR || process.cwd();
const STATE_FILE = path.join(STATE_DIR, '.indexing-state.json');
const MONITOR_STATE_FILE = path.join(STATE_DIR, '.monitor-state.json');
const DAILY_SUBMIT_LIMIT  = 200;
const DAILY_INSPECT_LIMIT = 2000;
const MAX_SUBMITS_PER_URL = 3;   // after 3 failed submissions, stop pinging — fix the page instead

// Google API endpoints
const INDEXING_API    = 'https://indexing.googleapis.com/v3/urlNotifications:publish';
const INSPECT_API     = 'https://searchconsole.googleapis.com/v1/urlInspection/index:inspect';
const ANALYTICS_API   = 'https://www.googleapis.com/webmasters/v3/sites';

// ─────────────────────────────────────────────────────────────
// CLI flags
// ─────────────────────────────────────────────────────────────
const args         = process.argv.slice(2);
const IS_SCAN      = args.includes('--scan') || args.includes('--scan-full');
const IS_FULL_SCAN = args.includes('--scan-full');  // weekly: GSC Analytics + crawler + CSV
const IS_STATUS    = args.includes('--status');
const IS_RECHECK   = args.includes('--recheck');
const SUBMIT_URL   = (() => { const i = args.indexOf('--submit-url'); return i >= 0 ? args[i+1] : null; })();
const DRY_RUN      = args.includes('--dry-run');

// ─────────────────────────────────────────────────────────────
// State management
// ─────────────────────────────────────────────────────────────
function loadState() {
  try {
    const s = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    // Migrate: ensure flagged array exists
    if (!s.flagged) s.flagged = [];
    // Migrate: ensure submitted entries have submitCount
    for (const entry of (s.submitted || [])) {
      if (!entry.submitCount) entry.submitCount = 1;
    }
    // Migrate: ensure prev_indexed/prev_not_indexed exist for delta tracking
    if (s.prev_indexed === undefined) s.prev_indexed = s.indexed?.length || 0;
    if (s.prev_not_indexed === undefined) s.prev_not_indexed = s.not_indexed?.length || 0;
    // Migrate: purge junk URLs (CDN images, query variants, feeds) that
    // previously leaked in via GSC Analytics / CSV import and burned quota
    const before = {
      not_indexed: s.not_indexed?.length || 0,
      submitted:   s.submitted?.length || 0,
      flagged:     s.flagged?.length || 0,
    };
    s.not_indexed = (s.not_indexed || []).filter(isIndexablePageUrl);
    s.indexed     = (s.indexed     || []).filter(isIndexablePageUrl);
    s.all_urls    = (s.all_urls    || []).filter(isIndexablePageUrl);
    s.submitted   = (s.submitted   || []).filter(e => isIndexablePageUrl(e.url));
    s.flagged     = (s.flagged     || []).filter(f => isIndexablePageUrl(f.url));
    // Keep flagged submitCount in sync with the live submission record
    for (const f of s.flagged) {
      const sub = s.submitted.find(x => x.url === f.url);
      if (sub && sub.submitCount > (f.submitCount || 0)) f.submitCount = sub.submitCount;
    }
    const purged = (before.not_indexed - s.not_indexed.length)
                 + (before.submitted - s.submitted.length)
                 + (before.flagged - s.flagged.length);
    if (purged > 0) console.log(`🧹 State cleanup: purged ${purged} junk URL entries (images/query variants/feeds)`);
    return s;
  } catch {
    return {
      all_urls:         [],    // all URLs from sitemap
      indexed:          [],    // confirmed indexed by GSC
      not_indexed:      [],    // confirmed NOT indexed
      submitted:        [],    // submitted to Indexing API (with timestamps + submitCount)
      flagged:          [],    // URLs submitted 2+ times, still not indexed, with diagnosis
      skipped:          [],    // redirects, errors, excluded
      last_scan:        null,
      last_submit_date: null,
      submitted_today:  0,
      inspected_today:  0,
      inspect_date:     null,
      prev_indexed:     0,     // yesterday's indexed count (for delta reporting)
      prev_not_indexed: 0,     // yesterday's not-indexed count
    };
  }
}

function saveState(s) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 2));
}

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

function resetDailyCountsIfNewDay(s) {
  const today = todayStr();
  if (s.last_submit_date !== today) {
    s.submitted_today  = 0;
    s.last_submit_date = today;
  }
  if (s.inspect_date !== today) {
    s.inspected_today = 0;
    s.inspect_date    = today;
  }
  return s;
}

// ─────────────────────────────────────────────────────────────
// Google Auth
// ─────────────────────────────────────────────────────────────
let _authClient = null;

async function getAuthClient() {
  if (_authClient) return _authClient;
  const saPath = path.resolve(process.cwd(), SA_PATH);
  if (!fs.existsSync(saPath)) {
    console.error(`\n❌  Service account file not found: ${saPath}`);
    console.error('   Set GOOGLE_SERVICE_ACCOUNT_PATH in .env (see README).\n');
    process.exit(1);
  }
  const auth = new GoogleAuth({
    keyFile: saPath,
    scopes: [
      'https://www.googleapis.com/auth/indexing',
      'https://www.googleapis.com/auth/webmasters.readonly',
    ],
  });
  _authClient = await auth.getClient();
  return _authClient;
}

async function authHeaders() {
  const client = await getAuthClient();
  const token  = await client.getAccessToken();
  return {
    'Authorization': `Bearer ${token.token}`,
    'Content-Type':  'application/json',
  };
}

// ─────────────────────────────────────────────────────────────
// GSC CSV import — import "Crawled not indexed" export from GSC
// Place the file as ./gsc-not-indexed.csv (exported from GSC Pages report)
// ─────────────────────────────────────────────────────────────
const GSC_CSV_FILE = path.join(STATE_DIR, 'gsc-not-indexed.csv');

function loadGscCsvUrls() {
  try {
    if (!fs.existsSync(GSC_CSV_FILE)) return [];
    const raw = fs.readFileSync(GSC_CSV_FILE, 'utf8');
    const lines = raw.split('\n').filter(l => l.trim());
    // GSC exports have a header row; URL is the first column
    const urls = [];
    for (let i = 1; i < lines.length; i++) {
      const cols = lines[i].split(',');
      const url = (cols[0] || '').trim().replace(/^"/, '').replace(/"$/, '');
      if (url.startsWith('http')) urls.push(url);
    }
    return urls.filter(isIndexablePageUrl);
  } catch {
    return [];
  }
}

// ─────────────────────────────────────────────────────────────
// Internal link crawler — discover URLs linked from the site
// that may not be in the sitemap (collections, tags, etc.)
// ─────────────────────────────────────────────────────────────
async function crawlInternalLinks(startUrl, maxPages = 50) {
  const domain = new URL(startUrl).origin;
  const discovered = new Set();
  const visited = new Set();
  const queue = [startUrl];

  // Also seed with common Shopify URL patterns
  const commonPaths = [
    '/collections', '/collections/all',
    `/blogs/${BLOG_HANDLE}`,
    '/pages/about', '/pages/contact',
  ];
  for (const p of commonPaths) {
    queue.push(domain + p);
  }

  while (queue.length > 0 && visited.size < maxPages) {
    const url = queue.shift();
    if (visited.has(url)) continue;
    visited.add(url);

    try {
      const res = await fetch(url, {
        headers: { 'User-Agent': USER_AGENT },
        redirect: 'follow',
        signal: AbortSignal.timeout(10000),
      });
      if (!res.ok) continue;
      const html = await res.text();

      // Extract all internal links via regex (fast, no DOM parser needed)
      const host = new URL(startUrl).hostname.replace(/^www\./, '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const linkRegex = new RegExp(`href=["'](\\/[^"'#?]*|https?:\\/\\/(?:www\\.)?${host}[^"'#?]*?)["']`, 'gi');
      let match;
      while ((match = linkRegex.exec(html)) !== null) {
        let href = match[1];
        if (href.startsWith('/')) href = domain + href;
        // Normalize
        href = href.replace(/\/$/, '');
        if (!href.startsWith(domain)) continue;
        // Skip assets, feeds, query strings
        if (/\.(js|css|png|jpg|jpeg|gif|svg|ico|woff|ttf|atom|json|xml)$/i.test(href)) continue;
        if (/\?(add-to-cart|v=|sf_paged|constraint)/.test(href)) continue;

        discovered.add(href.endsWith('/') ? href : href + '/');
        if (!visited.has(href) && !visited.has(href + '/')) {
          queue.push(href);
        }
      }
    } catch {
      // timeout or network error — skip
    }
  }

  return [...discovered];
}

// ─────────────────────────────────────────────────────────────
// GSC Search Analytics — query all pages Google knows about
// This returns every URL that has had impressions in search results
// ─────────────────────────────────────────────────────────────
async function fetchGscPages() {
  const headers = await authHeaders();
  const allPages = new Set();
  const encodedSite = encodeURIComponent(SITE_URL);

  // Query last 90 days of data to catch all pages Google tracks
  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - 90);

  const fmt = d => d.toISOString().slice(0, 10);
  let startRow = 0;
  const PAGE_SIZE = 5000;

  while (true) {
    const body = JSON.stringify({
      startDate: fmt(startDate),
      endDate: fmt(endDate),
      dimensions: ['page'],
      rowLimit: PAGE_SIZE,
      startRow,
    });

    const res = await fetch(
      `${ANALYTICS_API}/${encodedSite}/searchAnalytics/query`,
      { method: 'POST', headers, body }
    );

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      console.log(`   ⚠️  GSC Analytics API error: ${err?.error?.message || res.status}`);
      break;
    }

    const data = await res.json();
    const rows = data.rows || [];
    if (rows.length === 0) break;

    for (const row of rows) {
      if (row.keys?.[0]) allPages.add(row.keys[0]);
    }

    if (rows.length < PAGE_SIZE) break;
    startRow += PAGE_SIZE;
  }

  return [...allPages];
}

// ─────────────────────────────────────────────────────────────
// Sitemap parser
// ─────────────────────────────────────────────────────────────
async function fetchText(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res.text();
}

async function parseSitemap(url, depth = 0) {
  const xml    = await fetchText(url);
  const parser = new XMLParser({ ignoreAttributes: false });
  const doc    = parser.parse(xml);
  const urls   = [];

  if (doc.sitemapindex) {
    const maps = [].concat(doc.sitemapindex.sitemap || []);
    for (const m of maps) {
      const loc = typeof m === 'string' ? m : m.loc;
      if (loc) {
        process.stdout.write(`  → sub-sitemap: ${loc}\n`);
        const sub = await parseSitemap(loc, depth + 1);
        urls.push(...sub);
      }
    }
  }

  if (doc.urlset) {
    const entries = [].concat(doc.urlset.url || []);
    for (const e of entries) {
      const loc = typeof e === 'string' ? e : e.loc;
      if (loc) urls.push(loc.trim());
    }
  }

  return urls;
}

// ─────────────────────────────────────────────────────────────
// GSC URL Inspection API
// ─────────────────────────────────────────────────────────────
async function inspectUrl(url) {
  const headers = await authHeaders();
  const body    = JSON.stringify({
    inspectionUrl: url,
    siteUrl:       SITE_URL,
  });
  const res  = await fetch(INSPECT_API, { method: 'POST', headers, body });
  const data = await res.json();

  if (!res.ok) {
    if (res.status === 429) return { status: 'QUOTA_EXCEEDED' };
    return { status: 'ERROR', error: data?.error?.message || res.status };
  }

  const inspection = parseInspection(data);
  return { status: inspection.verdict, inspection };
}

// ─────────────────────────────────────────────────────────────
// Google Indexing API — submit URL
// ─────────────────────────────────────────────────────────────
async function submitUrl(url) {
  const headers = await authHeaders();
  const body    = JSON.stringify({ url, type: 'URL_UPDATED' });
  const res     = await fetch(INDEXING_API, { method: 'POST', headers, body });
  const data    = await res.json();

  if (!res.ok) {
    if (res.status === 429) return { ok: false, error: 'QUOTA_EXCEEDED' };
    return { ok: false, error: data?.error?.message || `HTTP ${res.status}` };
  }
  return { ok: true, notifyTime: data.urlNotificationMetadata?.latestUpdate?.notifyTime };
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ─────────────────────────────────────────────────────────────
// Read monitor state — the monitor runs before us and provides
// the freshest list of not-indexed URLs
// ─────────────────────────────────────────────────────────────
function loadMonitorFindings() {
  try {
    if (!fs.existsSync(MONITOR_STATE_FILE)) return { urls: [], available: false };
    const monitorState = JSON.parse(fs.readFileSync(MONITOR_STATE_FILE, 'utf8'));
    const urls = (monitorState.crawled_not_indexed || []).filter(isIndexablePageUrl);
    const lastRun = monitorState.last_run || 'never';
    return { urls, lastRun, available: true };
  } catch {
    return { urls: [], available: false };
  }
}

// ─────────────────────────────────────────────────────────────
// SCAN MODE — fetch sitemap, merge monitor findings, inspect NEW urls
// ─────────────────────────────────────────────────────────────
async function runScan() {
  console.log('\n' + '='.repeat(60));
  console.log('  SCAN: Fetching sitemap & syncing with monitor findings');
  console.log('='.repeat(60) + '\n');

  let state = loadState();
  state = resetDailyCountsIfNewDay(state);

  // 1. Parse sitemap
  const shopifySitemap = `https://${STORE}/sitemap.xml`;
  console.log(`Fetching sitemap: ${shopifySitemap}`);
  let allUrls;
  try {
    allUrls = await parseSitemap(shopifySitemap);
  } catch(e) {
    const domainSitemap = `${SITE_URL}sitemap.xml`;
    console.log(`  ↳ trying ${domainSitemap}`);
    allUrls = await parseSitemap(domainSitemap);
  }
  allUrls = [...new Set(allUrls)].filter(isIndexablePageUrl);
  console.log(`\nFound ${allUrls.length} URLs in sitemap`);

  // 2. Deep discovery sources (only on --scan-full, runs weekly)
  if (IS_FULL_SCAN) {
    // 2a. Query GSC Search Analytics for all pages Google knows about
    console.log('\n📊 Querying GSC Search Analytics (last 90 days)...');
    try {
      const gscPages = (await fetchGscPages()).filter(isIndexablePageUrl);
      const urlSetGsc = new Set(allUrls);
      let gscAdded = 0;
      for (const u of gscPages) {
        if (!urlSetGsc.has(u)) { allUrls.push(u); urlSetGsc.add(u); gscAdded++; }
      }
      console.log(`   Found ${gscPages.length} pages in GSC performance data (${gscAdded} not in sitemap)`);
    } catch (e) {
      console.log(`   ⚠️  GSC Analytics query failed: ${e.message}`);
    }

    // 2b. Import GSC CSV export (if available)
    const csvUrls = loadGscCsvUrls();
    if (csvUrls.length > 0) {
      const urlSet = new Set(allUrls);
      let csvAdded = 0;
      for (const u of csvUrls) {
        if (!urlSet.has(u)) { allUrls.push(u); urlSet.add(u); csvAdded++; }
      }
      console.log(`📄 GSC CSV: imported ${csvUrls.length} URLs (${csvAdded} new)`);
    }

    // 2c. Crawl internal links to discover URLs not in sitemap
    console.log('\n🕷️  Crawling internal links...');
    try {
      const crawledUrls = (await crawlInternalLinks(SITE_URL, 30)).filter(isIndexablePageUrl);
      const urlSetCrawl = new Set(allUrls);
      let crawlAdded = 0;
      for (const u of crawledUrls) {
        const normalized = u.endsWith('/') ? u : u + '/';
        const noSlash = u.replace(/\/$/, '');
        if (!urlSetCrawl.has(normalized) && !urlSetCrawl.has(noSlash)) {
          allUrls.push(normalized);
          urlSetCrawl.add(normalized);
          crawlAdded++;
        }
      }
      console.log(`   Discovered ${crawledUrls.length} internal links (${crawlAdded} new)`);
    } catch (e) {
      console.log(`   ⚠️  Crawl failed: ${e.message} — continuing with sitemap URLs`);
    }
  } else {
    console.log('\n💡 Light scan (sitemap + monitor only). Run --scan-full for deep discovery.');
  }

  // 3. Import monitor findings (the monitor already inspected URLs via API)
  const monitor = loadMonitorFindings();
  if (monitor.available) {
    console.log(`\n📋 Monitor findings (last run: ${monitor.lastRun}):`);
    console.log(`   ${monitor.urls.length} not-indexed URLs identified by monitor`);

    // Add monitor's not-indexed URLs directly to our not_indexed list
    const existingNotIndexed = new Set(state.not_indexed);
    let added = 0;
    for (const url of monitor.urls) {
      if (url && url.startsWith('http') && !existingNotIndexed.has(url)) {
        state.not_indexed.push(url);
        existingNotIndexed.add(url);
        added++;
      }
    }
    if (added > 0) console.log(`   + ${added} new not-indexed URLs added from monitor`);

    // Also remove any URLs the monitor found to be indexed
    // (the monitor re-checks known not-indexed URLs, so if they're no longer
    //  in the monitor's list, they may have been indexed)
    const monitorNotIndexedSet = new Set(monitor.urls);
    const beforeCount = state.not_indexed.length;
    state.not_indexed = state.not_indexed.filter(url => {
      // Keep it if: it's in the monitor's not-indexed list, OR
      // it wasn't checked by the monitor (not in sitemap or not in batch)
      return monitorNotIndexedSet.has(url) || !allUrls.includes(url);
    });
    const removed = beforeCount - state.not_indexed.length;
    if (removed > 0) console.log(`   - ${removed} URLs removed (now indexed per monitor)`);

    // Add monitor URLs to sitemap tracking if missing
    const urlSet = new Set(allUrls);
    for (const u of monitor.urls) {
      if (u && u.startsWith('http') && !urlSet.has(u)) {
        allUrls.push(u);
        urlSet.add(u);
      }
    }
  } else {
    console.log('\n⚠️  No monitor state found — will inspect URLs directly');
  }

  state.all_urls = allUrls;
  console.log(`Total: ${allUrls.length} URLs to track\n`);

  // 3. Find truly NEW URLs (not in any classification yet)
  //    This now includes sitemap + CSV import + crawled internal links + monitor
  const alreadyKnown = new Set([
    ...state.indexed,
    ...state.not_indexed,
    ...state.skipped,
  ]);

  const newUrls = allUrls.filter(u => !alreadyKnown.has(u));
  console.log(`Already classified: ${alreadyKnown.size}`);
  console.log(`New URLs to inspect: ${newUrls.length}`);

  // 4. Also check if any previously indexed URLs need refresh
  //    (re-check old indexed URLs periodically — every 7 days)
  const lastScanDate = state.last_scan ? new Date(state.last_scan) : new Date(0);
  const daysSinceLastScan = Math.floor((Date.now() - lastScanDate) / (1000 * 60 * 60 * 24));
  let refreshUrls = [];
  if (daysSinceLastScan >= 7 && !monitor.available) {
    // Only do full refresh if monitor isn't providing fresh data
    const maxRefresh = Math.min(state.indexed.length, DAILY_INSPECT_LIMIT - newUrls.length - state.inspected_today);
    if (maxRefresh > 0) {
      refreshUrls = state.indexed.slice(0, maxRefresh);
      console.log(`\n♻️  ${daysSinceLastScan} days since last full scan — re-checking ${refreshUrls.length} indexed URLs`);
    }
  }

  const toInspect = [...newUrls, ...refreshUrls];

  if (toInspect.length === 0 && monitor.available) {
    console.log('\n✅  No new URLs to inspect. Monitor data is fresh.');
    console.log(`    Not-indexed total: ${state.not_indexed.length}`);
    printSummary(state);
    saveState(state);
    return;
  }

  if (toInspect.length === 0) {
    console.log('\n✅  All URLs already classified. Run --recheck to re-scan.\n');
    printSummary(state);
    return;
  }

  // 5. Inspect each URL
  let indexed = 0, notIndexed = 0, errors = 0;

  for (let i = 0; i < toInspect.length; i++) {
    const url = toInspect[i];

    if (state.inspected_today >= DAILY_INSPECT_LIMIT) {
      console.log(`\n⚠️  Daily inspection limit (${DAILY_INSPECT_LIMIT}) reached. Re-run tomorrow.`);
      break;
    }

    const result = await inspectUrl(url);
    state.inspected_today++;

    if (result.status === 'QUOTA_EXCEEDED') {
      console.log('\n⚠️  Inspection quota exceeded. Re-run tomorrow.');
      break;
    }

    if (result.status === 'PASS') {
      // If it was in not_indexed before (refresh), remove it
      state.not_indexed = state.not_indexed.filter(u => u !== url);
      if (!state.indexed.includes(url)) state.indexed.push(url);
      indexed++;
    } else if (result.status === 'FAIL' || result.status === 'NEUTRAL') {
      if (!state.not_indexed.includes(url)) state.not_indexed.push(url);
      // Remove from indexed if it was there (de-indexed)
      state.indexed = state.indexed.filter(u => u !== url);
      notIndexed++;
    } else {
      if (!state.skipped.includes(url)) state.skipped.push(url);
      errors++;
    }

    const pct = Math.round(((i + 1) / toInspect.length) * 100);
    process.stdout.write(
      `\r  [${i+1}/${toInspect.length}] ${pct}%  ` +
      `indexed: ${indexed}  not-indexed: ${notIndexed}  errors: ${errors}   `
    );

    saveState(state);
    await sleep(1100);
  }

  state.last_scan = new Date().toISOString();
  saveState(state);

  console.log('\n');
  printSummary(state);
}

// ─────────────────────────────────────────────────────────────
// URL Diagnosis — now evidence-based, lives in gsc-lib.js diagnose().
// It combines the URL Inspection API result (coverage state, canonicals,
// robots, fetch state) with a live page audit (HTTP status, noindex,
// canonical tag, word count) instead of guessing from URL patterns.
// ─────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────
// RECHECK MODE — re-inspect submitted URLs to see if indexed
// ─────────────────────────────────────────────────────────────
async function runRecheck() {
  console.log('\n' + '='.repeat(60));
  console.log('  RECHECK: Re-inspecting previously submitted URLs');
  console.log('='.repeat(60) + '\n');

  let state = loadState();
  state = resetDailyCountsIfNewDay(state);

  const submittedUrls = state.submitted.map(s => s.url);
  console.log(`Checking ${submittedUrls.length} previously submitted URLs...\n`);

  if (submittedUrls.length === 0) {
    console.log('No previously submitted URLs to recheck.\n');
    printSummary(state);
    return;
  }

  let nowIndexed = 0, stillPending = 0;

  for (let i = 0; i < submittedUrls.length; i++) {
    const url = submittedUrls[i];

    if (state.inspected_today >= DAILY_INSPECT_LIMIT) {
      console.log(`\n⚠️  Daily inspection limit reached.`);
      break;
    }

    const result = await inspectUrl(url);
    state.inspected_today++;

    if (result.status === 'QUOTA_EXCEEDED') {
      console.log('\n⚠️  Quota exceeded.');
      break;
    }

    if (result.status === 'PASS') {
      state.not_indexed = state.not_indexed.filter(u => u !== url);
      if (!state.indexed.includes(url)) state.indexed.push(url);
      // Remove from submitted & flagged (it's done!)
      state.submitted = state.submitted.filter(s => s.url !== url);
      state.flagged = state.flagged.filter(f => f.url !== url);
      nowIndexed++;
      process.stdout.write(`\r  ✅ [${i+1}/${submittedUrls.length}] Now indexed: ${nowIndexed}  Still pending: ${stillPending}   `);
    } else {
      stillPending++;
      // Flag URLs that have been submitted 2+ times and still aren't indexed.
      // Diagnosis uses the REAL inspection result + a live page audit —
      // not URL-pattern guessing.
      const entry = state.submitted.find(s => s.url === url);
      if (entry && (entry.submitCount || 1) >= 2) {
        const existing = state.flagged.find(f => f.url === url);
        const staleDiagnosis = existing &&
          (!existing.diagnosedAt || (Date.now() - new Date(existing.diagnosedAt)) > 14 * 24 * 60 * 60 * 1000);

        if (!existing || staleDiagnosis) {
          const audit     = await auditPage(url);
          const diagnosis = diagnose(url, result.inspection || {}, audit);
          const flagEntry = {
            url,
            submitCount: entry.submitCount || 2,
            firstSubmitted: entry.firstSubmittedAt || entry.submittedAt,
            lastSubmitted: entry.submittedAt,
            flaggedAt: existing?.flaggedAt || new Date().toISOString(),
            diagnosedAt: new Date().toISOString(),
            cause: diagnosis.cause,
            fix: diagnosis.fix,
            severity: diagnosis.severity,
            auto: diagnosis.auto,
            evidence: {
              coverageState:  result.inspection?.coverageState || '',
              lastCrawlTime:  result.inspection?.lastCrawlTime || '',
              googleCanonical: result.inspection?.googleCanonical || '',
              httpStatus:     audit.httpStatus,
              wordCount:      audit.wordCount,
              noindex:        audit.noindex,
              canonicalMismatch: audit.canonicalMismatch,
            },
          };
          state.flagged = state.flagged.filter(f => f.url !== url);
          state.flagged.push(flagEntry);
          console.log(`\n  🚩 ${existing ? 'RE-DIAGNOSED' : 'FLAGGED'}: ${url}\n     Cause: ${diagnosis.cause}\n     Fix: ${diagnosis.fix}`);

          // Auto-untrack pages that are non-indexable by design
          // (redirects, canonicalized variants) — stop wasting quota on them
          if (diagnosis.auto === 'untrack') {
            state.not_indexed = state.not_indexed.filter(u => u !== url);
            state.submitted   = state.submitted.filter(s => s.url !== url);
            console.log('     ↳ Auto-untracked (non-indexable by design)');
          }
        } else if (existing) {
          existing.submitCount = entry.submitCount || existing.submitCount;
        }
      }
      process.stdout.write(`\r  ⏳ [${i+1}/${submittedUrls.length}] Now indexed: ${nowIndexed}  Still pending: ${stillPending}   `);
    }

    saveState(state);
    await sleep(1100);
  }

  console.log('\n');
  if (nowIndexed > 0) {
    console.log(`🎉  ${nowIndexed} URLs are now indexed!`);
  }
  if (stillPending > 0) {
    console.log(`⏳  ${stillPending} URLs still pending indexing.`);
  }
  console.log('');
  printSummary(state);
}

// ─────────────────────────────────────────────────────────────
// SUBMIT MODE — send next batch to Indexing API (200/day)
// ─────────────────────────────────────────────────────────────
async function runSubmit() {
  console.log('\n' + '='.repeat(60));
  console.log('  SUBMIT: Sending unindexed URLs to Google Indexing API');
  console.log('='.repeat(60) + '\n');

  let state = loadState();
  state = resetDailyCountsIfNewDay(state);

  // Also merge in any fresh monitor findings before submitting
  const monitor = loadMonitorFindings();
  if (monitor.available && monitor.urls.length > 0) {
    const existingNotIndexed = new Set(state.not_indexed);
    let added = 0;
    for (const url of monitor.urls) {
      if (url && url.startsWith('http') && !existingNotIndexed.has(url)) {
        state.not_indexed.push(url);
        existingNotIndexed.add(url);
        added++;
      }
    }
    if (added > 0) {
      console.log(`📋 Imported ${added} new not-indexed URLs from monitor\n`);
      saveState(state);
    }
  }

  if (state.not_indexed.length === 0) {
    console.log('✅  No unindexed URLs found. Everything looks good!\n');
    printSummary(state);
    return;
  }

  // URLs not yet submitted
  const alreadySubmittedUrls = new Set(state.submitted.map(s => s.url));
  const pending = state.not_indexed.filter(u => !alreadySubmittedUrls.has(u));

  // Re-submit URLs submitted > 7 days ago, still not indexed, AND under the
  // per-URL cap. Endless re-pinging does nothing — after MAX_SUBMITS_PER_URL
  // the URL stays flagged for a real fix instead.
  const sevenDaysAgo = Date.now() - (7 * 24 * 60 * 60 * 1000);
  const staleSubmissions = state.submitted.filter(s => {
    const subDate = new Date(s.submittedAt).getTime();
    return subDate < sevenDaysAgo
        && state.not_indexed.includes(s.url)
        && (s.submitCount || 1) < MAX_SUBMITS_PER_URL;
  });
  const staleUrls = staleSubmissions.map(s => s.url).filter(u => !pending.includes(u));
  const cappedOut = state.submitted.filter(s =>
    state.not_indexed.includes(s.url) && (s.submitCount || 1) >= MAX_SUBMITS_PER_URL).length;

  const allToSubmit = [...pending, ...staleUrls];

  const remaining_today = DAILY_SUBMIT_LIMIT - state.submitted_today;
  const batch           = allToSubmit.slice(0, remaining_today);

  console.log(`Not-indexed total:     ${state.not_indexed.length}`);
  console.log(`Never submitted:       ${pending.length}`);
  console.log(`Stale (>7d, <${MAX_SUBMITS_PER_URL}x):    ${staleUrls.length}`);
  console.log(`Capped (${MAX_SUBMITS_PER_URL}+ submits):  ${cappedOut} — awaiting content/technical fix, not re-pinged`);
  console.log(`Submitting today:      ${batch.length} (${state.submitted_today} already sent)\n`);

  if (batch.length === 0) {
    if (state.submitted_today >= DAILY_SUBMIT_LIMIT) {
      console.log(`⏸  Daily limit (${DAILY_SUBMIT_LIMIT}) already reached today.\n`);
    } else {
      console.log(`✅  All not-indexed URLs have been submitted! Waiting for Google to index.\n`);
    }
    printSummary(state);
    return;
  }

  let ok = 0, failed = 0;

  for (let i = 0; i < batch.length; i++) {
    const url = batch[i];

    if (DRY_RUN) {
      console.log(`  [dry-run] Would submit: ${url}`);
      ok++;
      continue;
    }

    const result = await submitUrl(url);

    if (result.ok) {
      // Track submitCount — increment if re-submitting
      const prev = state.submitted.find(s => s.url === url);
      const prevCount = prev ? (prev.submitCount || 1) : 0;
      const firstSubmittedAt = prev?.firstSubmittedAt || prev?.submittedAt || new Date().toISOString();
      state.submitted = state.submitted.filter(s => s.url !== url);
      state.submitted.push({
        url,
        submittedAt: new Date().toISOString(),
        firstSubmittedAt,
        notifyTime: result.notifyTime,
        submitCount: prevCount + 1,
      });
      state.submitted_today++;
      ok++;
    } else if (result.error === 'QUOTA_EXCEEDED') {
      console.log(`\n⚠️  Indexing API quota exceeded after ${ok} submissions.`);
      break;
    } else {
      console.error(`  ❌ ${url} → ${result.error}`);
      failed++;
    }

    process.stdout.write(`\r  [${i+1}/${batch.length}] ✓ ${ok}  ✗ ${failed}   `);

    saveState(state);
    await sleep(300);
  }

  console.log('\n');

  if (!DRY_RUN) {
    saveState(state);
    console.log(`✅  Submitted ${ok} URLs${failed > 0 ? `, ${failed} failed` : ''}`);
    const remainingPending = allToSubmit.length - batch.length;
    if (remainingPending > 0) {
      console.log(`⏭  ${remainingPending} remaining → run again tomorrow`);
    } else {
      console.log('🎉  All unindexed URLs have now been submitted!');
    }
  }

  console.log();
  printSummary(state);
}

// ─────────────────────────────────────────────────────────────
// SUBMIT SINGLE URL (--submit-url)
// ─────────────────────────────────────────────────────────────
async function runSubmitSingle(url) {
  console.log(`\nSubmitting: ${url}\n`);

  let state = loadState();
  state = resetDailyCountsIfNewDay(state);

  const result = await submitUrl(url);

  if (result.ok) {
    console.log(`✅  Submitted successfully`);
    console.log(`   Notify time: ${result.notifyTime || 'n/a'}`);
    const prev = state.submitted.find(s => s.url === url);
    const prevCount = prev ? (prev.submitCount || 1) : 0;
    const firstSubmittedAt = prev?.firstSubmittedAt || prev?.submittedAt || new Date().toISOString();
    state.submitted = state.submitted.filter(s => s.url !== url);
    state.submitted.push({
      url, submittedAt: new Date().toISOString(), firstSubmittedAt,
      notifyTime: result.notifyTime, submitCount: prevCount + 1,
    });
    state.submitted_today++;
    if (!state.not_indexed.includes(url) && !state.indexed.includes(url)) {
      state.not_indexed.push(url);
    }
    saveState(state);
  } else {
    console.error(`❌  Failed: ${result.error}`);
  }
}

// ─────────────────────────────────────────────────────────────
// Summary
// ─────────────────────────────────────────────────────────────
function printSummary(state) {
  const today           = todayStr();
  const indexed         = state.indexed.length;
  const notIndexed      = state.not_indexed.length;
  const totalTracked    = indexed + notIndexed + (state.skipped?.length || 0);
  const submitted       = state.submitted.length;
  const alreadySubmitted= new Set(state.submitted.map(s => s.url));
  const pendingSubmit   = state.not_indexed.filter(u => !alreadySubmitted.has(u)).length;
  const flagged         = state.flagged?.length || 0;

  // Delta calculations (compared to previous run)
  const prevIndexed     = state.prev_indexed || 0;
  const prevNotIndexed  = state.prev_not_indexed || 0;
  const deltaIndexed    = indexed - prevIndexed;
  const deltaNotIndexed = notIndexed - prevNotIndexed;

  const deltaStr = (n) => n > 0 ? ` (+${n})` : n < 0 ? ` (${n})` : '';

  console.log('');
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║              GSC INDEXING — DAILY REPORT                ║');
  console.log('╠══════════════════════════════════════════════════════════╣');
  console.log(`║  Indexed:        ${String(indexed).padEnd(6)}${deltaStr(deltaIndexed).padEnd(10)} of ${totalTracked} tracked URLs`);
  console.log(`║  Not indexed:    ${String(notIndexed).padEnd(6)}${deltaStr(deltaNotIndexed).padEnd(10)}`);
  console.log(`║  Submitted:      ${submitted} awaiting indexing`);
  if (pendingSubmit > 0) {
    console.log(`║  Pending submit: ${pendingSubmit} never-submitted URLs`);
  }
  console.log(`║  Today's quota:  ${state.last_submit_date === today ? state.submitted_today : 0} / ${DAILY_SUBMIT_LIMIT} submissions used`);
  console.log('╠══════════════════════════════════════════════════════════╣');

  // Flagged URLs section
  if (flagged > 0) {
    console.log(`║  🚩 FLAGGED URLS (${flagged}) — submitted 2+ times, still not indexed:`);
    console.log('║  ' + '─'.repeat(54));
    // Group by cause so the report reads as an action plan, not a URL dump
    const byCause = {};
    for (const f of state.flagged) {
      (byCause[f.cause] = byCause[f.cause] || []).push(f);
    }
    for (const [cause, list] of Object.entries(byCause).sort((a, b) => b[1].length - a[1].length)) {
      console.log(`║  ▸ ${cause}  (${list.length} URL${list.length > 1 ? 's' : ''})`);
      console.log(`║    Fix: ${list[0].fix}`);
      for (const f of list.slice(0, 10)) {
        const shortUrl = f.url.replace(SITE_URL.replace(/\/$/, ''), '');
        const ev = f.evidence
          ? ` [${[f.evidence.wordCount ? f.evidence.wordCount + 'w' : null,
                 f.evidence.lastCrawlTime ? 'crawled ' + f.evidence.lastCrawlTime.slice(0,10) : null,
                 f.submitCount + 'x submitted'].filter(Boolean).join(', ')}]`
          : ` [${f.submitCount}x submitted]`;
        console.log(`║      ${shortUrl}${ev}`);
      }
      if (list.length > 10) console.log(`║      … and ${list.length - 10} more`);
      console.log('║');
    }
  } else {
    console.log('║  ✅ No flagged URLs — all submissions progressing normally');
  }

  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log('');

  // Update prev counts for next run's delta calculation
  state.prev_indexed = indexed;
  state.prev_not_indexed = notIndexed;
  saveState(state);
}

// ─────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────
(async () => {
  if (IS_STATUS) {
    const state = loadState();
    resetDailyCountsIfNewDay(state);
    printSummary(state);
    return;
  }

  if (IS_SCAN) {
    await runScan();
    return;
  }

  if (IS_RECHECK) {
    await runRecheck();
    return;
  }

  if (SUBMIT_URL) {
    await runSubmitSingle(SUBMIT_URL);
    return;
  }

  // Default: submit next batch
  await runSubmit();
})();
