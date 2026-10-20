/**
 * gsc-lib.js — shared helpers for the indexing workflow
 *
 *  - isIndexablePageUrl(url): allowlist filter. Only real, canonical-able HTML
 *    pages pass. CDN images, query-string variants, feeds, auth pages etc. are
 *    rejected so they never enter state or burn API quota.
 *  - parseInspection(data): extract the FULL URL Inspection API result
 *    (coverage state, canonicals, robots, fetch state) instead of just PASS/FAIL.
 *  - auditPage(url): fetch the live page and measure what Google actually sees
 *    (HTTP status, redirects, noindex, canonical tag, word count, internal links).
 *  - diagnose(url, inspection, audit): evidence-based root cause + recommended
 *    fix, replacing the old URL-pattern guessing.
 */

require('dotenv').config();

// Hostname of the site being managed, e.g. "example.com" (no protocol, no www).
// Defaults to the host of GSC_SITE_URL so there is one source of truth.
const SITE_HOST = (process.env.SITE_HOST || (() => {
  try { return new URL(process.env.GSC_SITE_URL || 'https://example.com/').hostname; }
  catch { return 'example.com'; }
})()).replace(/^www\./, '');
const USER_AGENT = process.env.CRAWLER_USER_AGENT || 'Mozilla/5.0 (compatible; SEOIndexingBot/2.0)';

const escapeRegex = s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

// ─────────────────────────────────────────────────────────────
// URL filter — allowlist of indexable page types
// ─────────────────────────────────────────────────────────────
function isIndexablePageUrl(url) {
  let u;
  try { u = new URL(url); } catch { return false; }

  // Must be our domain, https
  if (u.protocol !== 'https:') return false;
  if (u.hostname !== SITE_HOST && u.hostname !== `www.${SITE_HOST}`) return false;

  // No query strings — ?page=, ?v=, ?width=, ?constraint=, ?add-to-cart= are
  // all non-canonical variants we never want to submit or track
  if (u.search) return false;

  const p = u.pathname.replace(/\/+$/, '') || '/';

  // Asset / feed / file extensions — includes all /cdn/shop/ images
  if (/\.(jpe?g|png|gif|webp|svg|ico|css|js|json|xml|atom|rss|pdf|txt|md|woff2?|ttf|mp4|webm|oembed)$/i.test(p)) return false;
  if (p.includes('.oembed')) return false;
  if (p.startsWith('/cdn/')) return false;

  // Auth / utility / trap pages
  if (/^\/(account|cart|checkout|challenge|password|search|apps|tools)(\/|$)/.test(p)) return false;
  if (p === '/pages/bot-trap') return false;

  // Allowlist: homepage + the four real Shopify content types.
  if (p === '/') return true;
  return /^\/(products|collections|blogs|pages)\//.test(p);
}

// ─────────────────────────────────────────────────────────────
// Parse the full URL Inspection API response
// https://developers.google.com/webmaster-tools/v1/urlInspection.index/inspect
// ─────────────────────────────────────────────────────────────
function parseInspection(data) {
  const r = data?.inspectionResult?.indexStatusResult || {};
  return {
    verdict:        r.verdict || 'UNKNOWN',          // PASS | FAIL | NEUTRAL
    coverageState:  r.coverageState || '',           // e.g. "Crawled - currently not indexed"
    robotsTxtState: r.robotsTxtState || '',          // ALLOWED | DISALLOWED
    indexingState:  r.indexingState || '',           // INDEXING_ALLOWED | BLOCKED_BY_META_TAG | ...
    pageFetchState: r.pageFetchState || '',          // SUCCESSFUL | SOFT_404 | NOT_FOUND | ...
    googleCanonical: r.googleCanonical || '',
    userCanonical:   r.userCanonical || '',
    lastCrawlTime:   r.lastCrawlTime || '',
    crawledAs:       r.crawledAs || '',
    sitemaps:        r.sitemap || [],
    referringUrls:   r.referringUrls || [],
  };
}

// ─────────────────────────────────────────────────────────────
// Live page audit — what does the page itself say?
// ─────────────────────────────────────────────────────────────
async function auditPage(url) {
  const audit = {
    httpStatus: 0, redirected: false, finalUrl: url,
    noindex: false, canonical: '', canonicalMismatch: false,
    wordCount: 0, internalLinks: 0, title: '', hasH1: false,
    error: null,
  };
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': USER_AGENT },
      redirect: 'follow',
      signal: AbortSignal.timeout(15000),
    });
    audit.httpStatus = res.status;
    audit.finalUrl   = res.url;
    audit.redirected = res.redirected;

    const xRobots = res.headers.get('x-robots-tag') || '';
    if (/noindex/i.test(xRobots)) audit.noindex = true;

    if (!res.ok) return audit;
    const html = await res.text();

    if (/<meta[^>]+name=["']robots["'][^>]+content=["'][^"']*noindex/i.test(html)) audit.noindex = true;

    const canonMatch = html.match(/<link[^>]+rel=["']canonical["'][^>]+href=["']([^"']+)["']/i)
                    || html.match(/<link[^>]+href=["']([^"']+)["'][^>]+rel=["']canonical["']/i);
    if (canonMatch) {
      audit.canonical = canonMatch[1];
      const norm = s => s.replace(/\/+$/, '').toLowerCase();
      audit.canonicalMismatch = norm(audit.canonical) !== norm(url);
    }

    const titleMatch = html.match(/<title[^>]*>([^<]*)<\/title>/i);
    if (titleMatch) audit.title = titleMatch[1].trim();
    audit.hasH1 = /<h1[\s>]/i.test(html);

    // Word count of main content (strip scripts/styles/tags)
    const text = html
      .replace(/<script[\s\S]*?<\/script>/gi, ' ')
      .replace(/<style[\s\S]*?<\/style>/gi, ' ')
      .replace(/<[^>]+>/g, ' ')
      .replace(/\s+/g, ' ');
    audit.wordCount = text.split(' ').filter(w => w.length > 2).length;

    // Total internal links on the page, as context for the diagnosis.
    const linkRe = new RegExp(`href=["'](\\/[^"'#?]+|https?:\\/\\/(?:www\\.)?${escapeRegex(SITE_HOST)}[^"'#?]*)["']`, 'gi');
    const linkMatches = html.match(linkRe) || [];
    audit.internalLinks = linkMatches.length;
  } catch (e) {
    audit.error = e.message;
  }
  return audit;
}

// ─────────────────────────────────────────────────────────────
// Evidence-based diagnosis
// ─────────────────────────────────────────────────────────────
function diagnose(url, inspection = {}, audit = {}) {
  const path = (() => { try { return new URL(url).pathname; } catch { return url; } })();

  // 1. Hard technical blockers first — these fully explain non-indexing
  if (audit.httpStatus === 404 || audit.httpStatus === 410 || inspection.pageFetchState === 'NOT_FOUND') {
    return { cause: `Page returns ${audit.httpStatus || 404}`, severity: 'fix-now',
      fix: 'Create a 301 redirect to the closest live page (or restore the page). Remove from sitemap.', auto: 'redirect' };
  }
  if (audit.redirected) {
    return { cause: `URL redirects to ${audit.finalUrl}`, severity: 'expected',
      fix: 'Not indexable by design. Update internal links/sitemap to point at the final URL, then stop tracking this one.', auto: 'untrack' };
  }
  if (audit.noindex || inspection.indexingState === 'BLOCKED_BY_META_TAG' || inspection.indexingState === 'BLOCKED_BY_HTTP_HEADER') {
    return { cause: 'Blocked by noindex (meta tag or X-Robots-Tag)', severity: 'verify',
      fix: 'If intentional, stop tracking. If not, remove the noindex directive.', auto: 'untrack-if-intentional' };
  }
  if (inspection.robotsTxtState === 'DISALLOWED') {
    return { cause: 'Blocked by robots.txt', severity: 'verify',
      fix: 'If intentional, stop tracking. If not, allow the path in robots.txt.', auto: 'untrack-if-intentional' };
  }
  if (audit.canonicalMismatch || (inspection.userCanonical && inspection.userCanonical !== url)) {
    const target = audit.canonical || inspection.userCanonical;
    return { cause: `Page canonicalizes to ${target}`, severity: 'expected',
      fix: 'Google is correctly consolidating to the canonical. Stop tracking this variant.', auto: 'untrack' };
  }
  if (inspection.googleCanonical && inspection.googleCanonical !== url) {
    return { cause: `Google chose a different canonical: ${inspection.googleCanonical}`, severity: 'fix-now',
      fix: 'Google sees this as a duplicate. Differentiate the content substantially, or 301/canonical it to the chosen page.', auto: 'review' };
  }
  if (inspection.pageFetchState && inspection.pageFetchState !== 'SUCCESSFUL' && inspection.pageFetchState !== 'PAGE_FETCH_STATE_UNSPECIFIED') {
    return { cause: `Google fetch problem: ${inspection.pageFetchState}`, severity: 'fix-now',
      fix: 'Check server/CDN behavior for Googlebot (soft 404, blocked resource, 5xx).', auto: 'review' };
  }

  // 2. Never crawled → discovery/demand problem
  if (inspection.coverageState === 'Discovered - currently not indexed' || !inspection.lastCrawlTime) {
    return { cause: 'Discovered but never crawled (low crawl demand)', severity: 'improve',
      fix: 'Add internal links from indexed, high-traffic pages; feature it in navigation or related-posts blocks.', auto: 'internal-links' };
  }

  // 3. Crawled but not selected → quality/duplication signals
  const evidence = [];
  if (audit.wordCount && audit.wordCount < 300) evidence.push(`thin content (${audit.wordCount} words)`);
  if (!audit.hasH1) evidence.push('missing H1');
  if (/-(copy|\d)$/.test(path)) evidence.push('slug suggests duplicate variant (-copy/-N)');

  if (path.startsWith('/products/') && /-(copy|\d)$/.test(path)) {
    return { cause: `Duplicate product variant${evidence.length ? ` — ${evidence.join(', ')}` : ''}`, severity: 'fix-now',
      fix: 'Delete or 301 the duplicate to the original product.', auto: 'redirect' };
  }
  if (audit.wordCount && audit.wordCount < 300) {
    return { cause: `Thin content — ${evidence.join(', ')}`, severity: 'improve',
      fix: path.startsWith('/blogs/') ? 'Queue for content rewrite (800+ words, unique angle, media, FAQ).'
         : path.startsWith('/collections/') ? 'Add a 150+ word unique collection description, or noindex if <4 products.'
         : 'Expand to 300+ words of unique, useful content.', auto: path.startsWith('/blogs/') ? 'requeue-blog' : 'review' };
  }

  return {
    cause: `Crawled but not selected (quality/demand)${evidence.length ? ` — ${evidence.join(', ')}` : ''}`,
    severity: 'improve',
    fix: `Content is adequate length${audit.wordCount ? ` (${audit.wordCount} words)` : ''} but Google isn't prioritizing it. ` +
         'Strengthen internal links from indexed pages, differentiate from similar pages, and verify it targets a query people search. ' +
         `Last crawled: ${inspection.lastCrawlTime ? inspection.lastCrawlTime.slice(0, 10) : 'unknown'}.`,
    auto: 'internal-links',
  };
}

module.exports = { isIndexablePageUrl, parseInspection, auditPage, diagnose, SITE_HOST };
