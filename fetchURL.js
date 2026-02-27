import fs from "fs";
import os from "os";
import path from "path";
import csv from "csv-parser";
import axios from "axios";
import puppeteer from "puppeteer";
import * as punycode from "punycode/punycode.js";

const DEFAULT_CSV_PATH = "domains.csv";
const AXIOS_TIMEOUT_MS = 45000;
const AXIOS_MAX_REDIRECTS = 20;
const AXIOS_RETRIES = 2;
const BROWSER_NAV_TIMEOUT_MS = 120000;
const BROWSER_SETTLE_MAX_MS = 90000;
const BROWSER_RETRIES = 1;
const USER_AGENT =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const ARTIFACTS_DIR = path.resolve("output", "fetch_artifacts");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function safeString(value) {
  return String(value ?? "").trim();
}

function safeFilename(value) {
  const base = safeString(value).toLowerCase().replace(/[^a-z0-9._-]+/g, "_");
  return base || "unknown-domain";
}

function ensureArtifactsDir() {
  fs.mkdirSync(ARTIFACTS_DIR, { recursive: true });
}

function normalizeHostname(hostname) {
  const raw = safeString(hostname).replace(/\.$/, "").toLowerCase();
  if (!raw) return "";
  try {
    return punycode.toASCII(raw);
  } catch {
    return raw;
  }
}

function ensureAbsoluteHttpUrl(value) {
  const raw = safeString(value);
  if (!raw) return null;

  try {
    const withProtocol = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
    const url = new URL(withProtocol);
    if (!/^https?:$/i.test(url.protocol)) return null;
    url.hash = "";
    return url.toString();
  } catch {
    return null;
  }
}

function flipWwwHost(urlValue) {
  try {
    const u = new URL(urlValue);
    const host = safeString(u.hostname).toLowerCase();
    if (!host) return null;
    u.hostname = host.startsWith("www.") ? host.replace(/^www\./, "") : `www.${host}`;
    return u.toString();
  } catch {
    return null;
  }
}

function parseDomainInput(rawValue) {
  const raw = safeString(rawValue)
    .replace(/^['"]+|['"]+$/g, "")
    .replace(/^\s+|\s+$/g, "");

  if (!raw) return null;

  const preferredUrl = ensureAbsoluteHttpUrl(raw);
  if (preferredUrl) {
    const u = new URL(preferredUrl);
    u.hostname = normalizeHostname(u.hostname);
    return {
      original: raw,
      preferredUrl: u.toString(),
      host: u.hostname,
      port: u.port || null,
      path: `${u.pathname || "/"}${u.search || ""}`,
    };
  }

  const normalizedHost = normalizeHostname(raw.replace(/\/.*$/, ""));
  if (!normalizedHost) return null;

  return {
    original: raw,
    preferredUrl: null,
    host: normalizedHost,
    port: null,
    path: "/",
  };
}

function buildCandidateUrls(parsed) {
  const out = [];
  const seen = new Set();
  const push = (value) => {
    const normalized = ensureAbsoluteHttpUrl(value);
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    out.push(normalized);
  };

  if (parsed.preferredUrl) {
    push(parsed.preferredUrl);
  }

  const host = parsed.host;
  const hostNoWww = host.replace(/^www\./, "");
  const hostWww = hostNoWww.startsWith("www.") ? hostNoWww : `www.${hostNoWww}`;
  const hostVariants = new Set([host, hostNoWww, hostWww]);
  const path = parsed.path || "/";

  for (const protocol of ["https", "http"]) {
    for (const h of hostVariants) {
      if (!h) continue;
      const portPart = parsed.port ? `:${parsed.port}` : "";
      push(`${protocol}://${h}${portPart}${path}`);
    }
  }

  return out;
}

function isHttpUrl(value) {
  if (!value) return false;
  try {
    const u = new URL(value);
    return /^https?:$/i.test(u.protocol);
  } catch {
    return false;
  }
}

function isTransientErrorCode(code) {
  const c = safeString(code).toUpperCase();
  return (
    c === "ETIMEDOUT" ||
    c === "ECONNABORTED" ||
    c === "ECONNRESET" ||
    c === "EPIPE" ||
    c === "ERR_HTTP2_GOAWAY_SESSION" ||
    c === "ERR_HTTP2_STREAM_CANCEL"
  );
}

function isDefinitiveStopErrorCode(code) {
  const c = safeString(code).toUpperCase();
  return (
    c === "ENOTFOUND" ||
    c === "ENODATA" ||
    c === "ESERVFAIL" ||
    c === "ECONNREFUSED" ||
    c === "ERR_SSL_NO_CERTIFICATE" ||
    c === "ERR_TLS_CERT_ALTNAME_INVALID" ||
    c === "ERR_CERT_COMMON_NAME_INVALID" ||
    c === "ERR_CERT_AUTHORITY_INVALID" ||
    c === "DEPTH_ZERO_SELF_SIGNED_CERT" ||
    c === "SELF_SIGNED_CERT_IN_CHAIN" ||
    c === "EMPTY_DOMAIN"
  );
}

async function classifyHttpResponse(domain) {
  const normalizedDomain = normalizeHostname(domain);
  if (!normalizedDomain) {
    return {
      domain_status: "Does not resolve_S1",
      status: 0,
      resolvingUrl: null,
      error: "EMPTY_DOMAIN",
    };
  }

  const startUrl = `https://${normalizedDomain}`;
  const attemptRequest = async (url) => {
    const res = await axios.get(url, {
      timeout: AXIOS_TIMEOUT_MS,
      maxRedirects: AXIOS_MAX_REDIRECTS,
      validateStatus: () => true,
      headers: {
        "User-Agent": USER_AGENT,
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
    });

    const status = Number(res?.status || 0);
    const resolvingUrl =
      res?.request?.res?.responseUrl ||
      res?.request?._redirectable?._currentUrl ||
      res?.config?.url ||
      url;

    return { status, resolvingUrl };
  };

  const mapStatusToDecision = (status, resolvingUrl) => {
    if (status === 401 || status === 403) {
      return { domain_status: "PRIVATE_S1_AuthBlocked", status, resolvingUrl, error: null };
    }
    if (status >= 500) {
      return {
        domain_status: "PARKED_S1_ServerError",
        status,
        resolvingUrl,
        error: null,
      };
    }
    if (status >= 200 && status < 500) {
      return { domain_status: "ACTIVE_S1_HTTPReachable", status, resolvingUrl, error: null };
    }
    return { domain_status: null, status, resolvingUrl, error: null };
  };

  try {
    const primary = await attemptRequest(startUrl);
    const primaryDecision = mapStatusToDecision(primary.status, primary.resolvingUrl);

    // If auth-blocked on one host, try the sibling host (www/non-www) before declaring inactive.
    if (primary.status === 401 || primary.status === 403) {
      const alternateUrl = flipWwwHost(startUrl);
      if (alternateUrl && alternateUrl !== startUrl) {
        try {
          const alternate = await attemptRequest(alternateUrl);
          if (alternate.status >= 200 && alternate.status < 500 && alternate.status !== 401 && alternate.status !== 403) {
            return mapStatusToDecision(alternate.status, alternate.resolvingUrl);
          }
        } catch {
          // Keep original decision when alternate host also fails.
        }
      }
    }

    return primaryDecision;
  } catch (error) {
    const code = safeString(error?.code || error?.name || "AXIOS_ERROR");
    if (isDefinitiveStopErrorCode(code)) {
      const alternateUrl = flipWwwHost(startUrl);
      if (alternateUrl && alternateUrl !== startUrl) {
        try {
          const alternate = await attemptRequest(alternateUrl);
          return mapStatusToDecision(alternate.status, alternate.resolvingUrl);
        } catch {
          // Fall through to "does not resolve" using original code.
        }
      }
      return {
        domain_status: "Does not resolve_S1",
        status: 0,
        resolvingUrl: null,
        error: code,
      };
    }
    return { domain_status: null, status: 0, resolvingUrl: startUrl, error: code };
  }
}

async function fetchHTMLAxios(inputUrlOrDomain, domainForFallback = null) {
  let startUrl = ensureAbsoluteHttpUrl(inputUrlOrDomain);
  if (!startUrl && domainForFallback) {
    const host = normalizeHostname(domainForFallback);
    startUrl = host ? `https://${host}` : null;
  }

  if (!startUrl) {
    return {
      ok: false,
      html: "",
      resolvingUrl: null,
      finalUrl: null,
      status: null,
      headers: {},
      errorCode: "BAD_URL",
      errorMessage: "Invalid URL",
      usedFallback: false,
    };
  }

  try {
    const res = await axios.get(startUrl, {
      timeout: AXIOS_TIMEOUT_MS,
      maxRedirects: AXIOS_MAX_REDIRECTS,
      responseType: "arraybuffer",
      validateStatus: () => true,
      headers: {
        "User-Agent": USER_AGENT,
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "identity",
      },
    });

    const finalUrl =
      res?.request?.res?.responseUrl ||
      res?.request?._redirectable?._currentUrl ||
      res?.config?.url ||
      startUrl;

    const html = Buffer.isBuffer(res?.data)
      ? res.data.toString("utf8")
      : String(res?.data ?? "");

    return {
      ok: true,
      html,
      resolvingUrl: finalUrl,
      finalUrl,
      status: typeof res.status === "number" ? res.status : null,
      headers: res.headers || {},
      errorCode: null,
      errorMessage: null,
      usedFallback: false,
    };
  } catch (error) {
    const code = safeString(error?.code || error?.name || "FETCH_ERROR");
    return {
      ok: false,
      html: "",
      resolvingUrl: startUrl,
      finalUrl: startUrl,
      status: null,
      headers: {},
      errorCode: code,
      errorMessage: safeString(error?.message || code),
      usedFallback: false,
    };
  }
}

function classifyNonRenderedHtml(html) {
  const htmlLower = safeString(html).toLowerCase();
  if (!htmlLower) {
    return { domain_status: "UNDERDEV_S3_emptyHtml", needsRendering: false };
  }
  if (
    htmlLower.includes("this domain is for sale") ||
    htmlLower.includes("buy this domain") ||
    htmlLower.includes("parkingcrew.net") ||
    htmlLower.includes("sedo.com")
  ) {
    return { domain_status: "PARKED_S3_placeholder", needsRendering: false };
  }
  return { domain_status: null, needsRendering: false };
}

async function fetchHTMLFinalHtml(initialUrl, initialHtml, needsRendering) {
  if (!needsRendering) {
    return {
      html: initialHtml || "",
      lowerCaseHtml: safeString(initialHtml).toLowerCase(),
      resolvingUrl: initialUrl || null,
      usedPuppeteer: false,
    };
  }

  return {
    html: initialHtml || "",
    lowerCaseHtml: safeString(initialHtml).toLowerCase(),
    resolvingUrl: initialUrl || null,
    usedPuppeteer: false,
  };
}

function extractFeatures(html, domain) {
  const text = safeString(html).replace(/<[^>]+>/g, " ");
  const words = text ? text.split(/\s+/).filter(Boolean) : [];
  const domainLower = safeString(domain).toLowerCase();
  const lowerHtml = safeString(html).toLowerCase();

  return {
    countWords_Active: words.length,
    hasMenu_ACTIVE: lowerHtml.includes("<nav") || lowerHtml.includes("menu"),
    hasNavigation_ACTIVE: lowerHtml.includes("<nav") || lowerHtml.includes("href="),
    hasContactPage_Active: lowerHtml.includes("contact"),
    hasDomainForSaleMessage_PARKED:
      lowerHtml.includes("domain is for sale") || lowerHtml.includes("buy this domain"),
    hasMarketPlaceBrand_PARKED:
      lowerHtml.includes("sedo") ||
      lowerHtml.includes("afternic") ||
      lowerHtml.includes("godaddy"),
    hasAuthComponent:
      lowerHtml.includes("type=\"password\"") || lowerHtml.includes("login"),
    hasAuthEndpoint:
      lowerHtml.includes("/login") || lowerHtml.includes("/signin"),
    metaTitle: domainLower,
  };
}

function classifyStage6(features, htmlFinal) {
  const htmlLower = safeString(htmlFinal).toLowerCase();
  if (!htmlLower) return "UNDERDEV_S6_emptyHtml";
  if (features.hasDomainForSaleMessage_PARKED || features.hasMarketPlaceBrand_PARKED) {
    return "PARKED_S6_ForSale";
  }
  if (features.hasAuthComponent && features.hasAuthEndpoint) {
    return "PRIVATE_S6_LoginGate";
  }
  if ((features.countWords_Active || 0) >= 80) return "ACTIVE_S6_ContentDetected";
  return "SendToClassifier4_Ml";
}

async function probeWithAxios(startUrl) {
  for (let attempt = 0; attempt <= AXIOS_RETRIES; attempt += 1) {
    try {
      const res = await axios.get(startUrl, {
        timeout: AXIOS_TIMEOUT_MS,
        maxRedirects: AXIOS_MAX_REDIRECTS,
        validateStatus: () => true,
        headers: {
          "User-Agent": USER_AGENT,
          Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
      });

      const finalUrl =
        res?.request?.res?.responseUrl ||
        res?.request?._redirectable?._currentUrl ||
        res?.config?.url ||
        startUrl;

      return {
        ok: true,
        startUrl,
        finalUrl: isHttpUrl(finalUrl) ? finalUrl : startUrl,
        status: typeof res.status === "number" ? res.status : null,
        errorCode: null,
      };
    } catch (error) {
      const errorCode = safeString(error?.code || error?.name || "AXIOS_ERROR");
      const canRetry = attempt < AXIOS_RETRIES && isTransientErrorCode(errorCode);
      if (canRetry) {
        await sleep(1200 * (attempt + 1));
        continue;
      }

      return {
        ok: false,
        startUrl,
        finalUrl: null,
        status: null,
        errorCode,
      };
    }
  }
}

function scoreProbeResult(result) {
  if (!result.ok) return 0;
  const status = result.status ?? 0;
  let score = 50;

  if (status >= 200 && status < 400) score += 60;
  else if (status === 401 || status === 403) score += 50;
  else if (status >= 400 && status < 600) score += 30;

  if (result.finalUrl && result.finalUrl !== result.startUrl) score += 20;
  if ((result.finalUrl || "").startsWith("https://")) score += 10;
  if ((result.errorCode || "").includes("ERR_CERT")) score -= 5;

  return score;
}

async function settleClientRedirects(page) {
  const startTime = Date.now();
  let lastUrl = page.url();
  let stableChecks = 0;

  while (Date.now() - startTime < BROWSER_SETTLE_MAX_MS) {
    try {
      await page.waitForNavigation({
        waitUntil: "domcontentloaded",
        timeout: 8000,
      });
    } catch {
      // Expected when no new navigation happens.
    }

    // Handle meta refresh redirects where browser may wait before navigation.
    try {
      const metaRefresh = await page.evaluate(() => {
        const tags = Array.from(
          document.querySelectorAll('meta[http-equiv="refresh" i]')
        );
        for (const tag of tags) {
          const content = (tag.getAttribute("content") || "").trim();
          if (!content) continue;
          const match = content.match(/^\s*(\d+)\s*;\s*url\s*=\s*(.+)\s*$/i);
          if (!match) continue;
          const seconds = Number(match[1]);
          const target = match[2].replace(/^['"]|['"]$/g, "").trim();
          if (!target) continue;
          if (Number.isFinite(seconds) && seconds <= 30) {
            return { seconds, target };
          }
        }
        return null;
      });

      if (metaRefresh?.target) {
        const nextUrl = new URL(metaRefresh.target, page.url()).toString();
        if (isHttpUrl(nextUrl) && nextUrl !== page.url()) {
          await page.goto(nextUrl, {
            waitUntil: "domcontentloaded",
            timeout: BROWSER_NAV_TIMEOUT_MS,
          });
        }
      }
    } catch {
      // Ignore if page context changed or target URL is invalid.
    }

    const currentUrl = page.url();
    if (currentUrl === lastUrl) {
      stableChecks += 1;
    } else {
      stableChecks = 0;
      lastUrl = currentUrl;
    }

    if (stableChecks >= 2) break;
    await sleep(1800);
  }
}

async function resolveInBrowser(browser, startUrl) {
  let lastFailure = null;

  for (let attempt = 0; attempt <= BROWSER_RETRIES; attempt += 1) {
    const page = await browser.newPage();
    const navTrail = [];
    const pushTrail = (url) => {
      if (!isHttpUrl(url)) return;
      if (!navTrail.includes(url)) navTrail.push(url);
    };

    page.setDefaultNavigationTimeout(BROWSER_NAV_TIMEOUT_MS);
    await page.setUserAgent(USER_AGENT);
    await page.setBypassCSP(true);

    page.on("framenavigated", (frame) => {
      if (frame === page.mainFrame()) {
        pushTrail(frame.url());
      }
    });

    page.on("response", (response) => {
      const request = response.request();
      if (
        request.isNavigationRequest() &&
        request.frame() === page.mainFrame()
      ) {
        pushTrail(response.url());
      }
    });

    try {
      pushTrail(startUrl);
      await page.goto(startUrl, {
        waitUntil: "domcontentloaded",
        timeout: BROWSER_NAV_TIMEOUT_MS,
      });
      pushTrail(page.url());

      await settleClientRedirects(page);
      pushTrail(page.url());

      const finalUrl = navTrail[navTrail.length - 1] || page.url() || startUrl;
      return {
        ok: isHttpUrl(finalUrl),
        finalUrl: isHttpUrl(finalUrl) ? finalUrl : null,
        navTrail,
        errorCode: null,
      };
    } catch (error) {
      const errorCode = safeString(error?.code || error?.name || "BROWSER_ERROR");
      lastFailure = {
        ok: false,
        finalUrl: navTrail[navTrail.length - 1] || null,
        navTrail,
        errorCode,
      };

      if (attempt < BROWSER_RETRIES && isTransientErrorCode(errorCode)) {
        await sleep(1500 * (attempt + 1));
      } else {
        return lastFailure;
      }
    } finally {
      await page.close().catch(() => {});
    }
  }

  return (
    lastFailure || {
      ok: false,
      finalUrl: null,
      navTrail: [],
      errorCode: "BROWSER_ERROR",
    }
  );
}

function uniqueHttpUrls(values) {
  const out = [];
  const seen = new Set();
  for (const value of values) {
    if (!isHttpUrl(value)) continue;
    if (seen.has(value)) continue;
    seen.add(value);
    out.push(value);
  }
  return out;
}

function isStopStatus(status) {
  const s = safeString(status).toUpperCase();
  if (!s) return false;

  return (
    s.includes("DOES NOT RESOLVE") ||
    s.includes("DOES_NOT_RESOLVE") ||
    s.startsWith("PARKED_") ||
    s.startsWith("UNDERDEV_") ||
    s.startsWith("PRIVATE_") ||
    s.startsWith("PRIVATESITE_") ||
    s.startsWith("BLOCKED") ||
    s.includes("SUSPENDED")
  );
}

function isActiveStatus(status) {
  const s = safeString(status).toUpperCase();
  if (!s) return false;
  return s.startsWith("ACTIVE_");
}

function decideFromStageStatus(status) {
  if (isActiveStatus(status)) return true;
  if (isStopStatus(status)) return false;
  return null;
}

async function evaluateActiveSite(domain, resolvingURL) {
  const result = {
    activeSite: true,
    stage1Status: null,
    stage1DomainStatus: null,
    stage1Error: null,
    stage2Status: null,
    stage2Error: null,
    stage3DomainStatus: null,
    stage6DomainStatus: null,
    usedPuppeteer: false,
    initialResolvedUrl: resolvingURL || null,
    finalResolvedUrl: resolvingURL || null,
    stage2Html: "",
    finalHtml: "",
    lowerCaseHtml: "",
  };

  // Stage 1 (file 1) — HTTP response classifier
  let stage1;
  try {
    stage1 = await classifyHttpResponse(domain);
  } catch {
    return result;
  }

  result.stage1Status = Number(stage1?.status || 0);
  result.stage1DomainStatus = safeString(stage1?.domain_status) || null;
  result.stage1Error = safeString(stage1?.error) || null;
  result.initialResolvedUrl =
    resolvingURL || safeString(stage1?.resolvingUrl) || result.initialResolvedUrl;

  const normalizedInputHost = normalizeHostname(
    safeString(domain).replace(/^https?:\/\//i, "").replace(/\/.*$/, "")
  );
  let resolvedHost = "";
  try {
    if (isHttpUrl(resolvingURL)) {
      resolvedHost = normalizeHostname(new URL(resolvingURL).hostname);
    }
  } catch {
    resolvedHost = "";
  }
  const hasDifferentResolvedHost =
    !!resolvedHost && !!normalizedInputHost && resolvedHost !== normalizedInputHost;

  const stage1Decision = decideFromStageStatus(stage1?.domain_status);
  if (stage1Decision !== null && !(stage1Decision === false && hasDifferentResolvedHost)) {
    result.activeSite = stage1Decision;
    return result;
  }

  const stage1Status = result.stage1Status;
  const stage1Error = result.stage1Error || "";
  if (stage1Status >= 500 && !hasDifferentResolvedHost) {
    result.activeSite = false;
    return result;
  }
  if (stage1Status === 401 && !hasDifferentResolvedHost) {
    result.activeSite = false;
    return result;
  }
  if (stage1Status === 0 && isDefinitiveStopErrorCode(stage1Error) && !hasDifferentResolvedHost) {
    result.activeSite = false;
    return result;
  }
  if (stage1Error === "AUTH_REQUIRED_401" && !hasDifferentResolvedHost) {
    result.activeSite = false;
    return result;
  }

  // Stage 2 fetch — needed as input to Stage 3 and Stage 6
  const stage2InputUrl =
    resolvingURL || stage1?.resolvingUrl || `https://${domain}`;
  let stage2;
  try {
    stage2 = await fetchHTMLAxios(stage2InputUrl, domain);
  } catch {
    return result;
  }

  const stage2Status = Number(stage2?.status || 0);
  result.stage2Status = stage2Status;
  result.stage2Error = safeString(stage2?.errorCode) || null;
  if (stage2Status >= 500) {
    result.activeSite = false;
    return result;
  }
  if (!stage2?.ok && !safeString(stage2?.html)) {
    if (isDefinitiveStopErrorCode(stage2?.errorCode)) {
      result.activeSite = false;
      return result;
    }
    return result;
  }

  const stage2Html = typeof stage2?.html === "string" ? stage2.html : "";
  result.stage2Html = stage2Html;
  const initialResolvedUrl =
    safeString(stage2?.resolvingUrl) ||
    safeString(stage2?.finalUrl) ||
    stage2InputUrl;
  result.initialResolvedUrl = initialResolvedUrl;

  // Stage 3 (file 3) — non-rendered classifier
  let stage3;
  try {
    stage3 = classifyNonRenderedHtml(stage2Html, initialResolvedUrl);
  } catch {
    result.finalHtml = stage2Html;
    result.lowerCaseHtml = stage2Html.toLowerCase();
    result.finalResolvedUrl = initialResolvedUrl;
    return result;
  }
  result.stage3DomainStatus = safeString(stage3?.domain_status) || null;
  const stage3Decision = decideFromStageStatus(stage3?.domain_status);
  if (stage3Decision !== null) {
    result.activeSite = stage3Decision;
    result.finalHtml = stage2Html;
    result.lowerCaseHtml = stage2Html.toLowerCase();
    result.finalResolvedUrl = initialResolvedUrl;
    return result;
  }

  // Stage 4 render (only when Stage 3 asks for it), to feed Stage 6
  let htmlFinal = stage2Html;
  let lowerCaseHtml = stage2Html.toLowerCase();
  let finalResolvedUrl = initialResolvedUrl;
  let usedPuppeteer = false;

  if (stage3?.needsRendering) {
    try {
      const stage4 = await fetchHTMLFinalHtml(initialResolvedUrl, stage2Html, true);
      if (safeString(stage4?.html)) {
        htmlFinal = stage4.html;
        lowerCaseHtml =
          typeof stage4?.lowerCaseHtml === "string"
            ? stage4.lowerCaseHtml
            : stage4.html.toLowerCase();
        finalResolvedUrl =
          safeString(stage4?.resolvingUrl) || initialResolvedUrl;
        usedPuppeteer = !!stage4?.usedPuppeteer;
      }
    } catch {
      result.finalHtml = stage2Html;
      result.lowerCaseHtml = stage2Html.toLowerCase();
      result.finalResolvedUrl = initialResolvedUrl;
      return result;
    }
  }

  result.usedPuppeteer = usedPuppeteer;
  result.finalHtml = htmlFinal;
  result.lowerCaseHtml = lowerCaseHtml;
  result.finalResolvedUrl = finalResolvedUrl;

  // Stage 6 (file 6) — weighted classifier
  const features = extractFeatures(htmlFinal, domain, {
    lowerCaseHtml,
    usedPuppeteer,
  });
  features.resolvingUrl = finalResolvedUrl;
  features.__resolvingUrl = finalResolvedUrl;

  let stage6Status;
  try {
    stage6Status = classifyStage6(features, htmlFinal);
  } catch {
    return result;
  }
  result.stage6DomainStatus = safeString(stage6Status) || null;
  const stage6Decision = decideFromStageStatus(stage6Status);
  if (stage6Decision !== null) {
    result.activeSite = stage6Decision;
    return result;
  }

  // "SendToClassifier4_Ml" or unknown here means "keep investigating"
  return result;
}

async function resolveDomainToFinalUrl(browser, rawDomain) {
  const parsed = parseDomainInput(rawDomain);
  if (!parsed) return null;

  const candidateUrls = buildCandidateUrls(parsed);
  if (!candidateUrls.length) return null;

  const probeResults = await Promise.all(candidateUrls.map(probeWithAxios));
  const sortedProbeResults = [...probeResults].sort(
    (a, b) => scoreProbeResult(b) - scoreProbeResult(a)
  );

  const browserCandidates = uniqueHttpUrls([
    ...sortedProbeResults.map((r) => r.finalUrl),
    ...sortedProbeResults.map((r) => r.startUrl),
    ...candidateUrls,
  ]);

  if (browser) {
    for (const startUrl of browserCandidates) {
      const result = await resolveInBrowser(browser, startUrl);
      if (result.ok && result.finalUrl) {
        return result.finalUrl;
      }
    }
  }

  const bestProbe = sortedProbeResults.find((r) => r.ok && isHttpUrl(r.finalUrl));
  return bestProbe?.finalUrl || candidateUrls[0] || null;
}

function readCsvRows(csvPath) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(csvPath)
      .pipe(csv())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

function extractInputFields(row) {
  const values = Object.values(row || {});
  const rawDomain = safeString(values[0]);
  const description1 = safeString(values[1]);
  const description2 = safeString(values[2]);
  return { rawDomain, description1, description2 };
}

async function main() {
  const csvPath = process.argv[2] || DEFAULT_CSV_PATH;
  const limitArg = Number(process.argv[3]);
  const rowLimit = Number.isFinite(limitArg) && limitArg > 0 ? limitArg : null;

  const rows = await readCsvRows(csvPath);
  const targets = rowLimit ? rows.slice(0, rowLimit) : rows;
  ensureArtifactsDir();

  const userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), "hubspot-url-enricher-"));

  let browser = null;
  try {
    browser = await puppeteer.launch({
      headless: "new",
      ignoreHTTPSErrors: true,
      protocolTimeout: 90000,
      userDataDir,
      env: {
        ...process.env,
        HOME: userDataDir,
        XDG_CONFIG_HOME: userDataDir,
        XDG_CACHE_HOME: userDataDir,
      },
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--ignore-certificate-errors",
        "--ignore-certificate-errors-spki-list",
        "--disable-features=IsolateOrigins,site-per-process",
        "--disable-crashpad",
        "--disable-breakpad",
      ],
    });
  } catch (error) {
    console.error(
      `⚠️ Puppeteer unavailable, continuing with Axios-only URL resolution: ${safeString(
        error?.message || error
      )}`
    );
  }

  try {
    for (const row of targets) {
      const { rawDomain, description1, description2 } = extractInputFields(row);
      if (!rawDomain) continue;

      try {
        const resolvingURL = await resolveDomainToFinalUrl(browser, rawDomain);
        const evaluation = await evaluateActiveSite(rawDomain, resolvingURL);
        const activeSite = !!evaluation.activeSite;
        const finalResolvingUrl =
          safeString(evaluation.finalResolvedUrl) ||
          safeString(resolvingURL) ||
          null;

        const safeDomain = safeFilename(rawDomain);
        const stage2HtmlPath = path.join(ARTIFACTS_DIR, `${safeDomain}_stage2.html`);
        const finalHtmlPath = path.join(ARTIFACTS_DIR, `${safeDomain}_final.html`);
        const metadataPath = path.join(ARTIFACTS_DIR, `${safeDomain}.json`);

        fs.writeFileSync(stage2HtmlPath, evaluation.stage2Html || "", "utf8");
        fs.writeFileSync(
          finalHtmlPath,
          (evaluation.finalHtml || evaluation.stage2Html || ""),
          "utf8"
        );

        const metadata = {
          rawDomain,
          resolvingURL: finalResolvingUrl,
          activeSite,
          description1,
          description2,
          stage1Status: evaluation.stage1Status,
          stage1DomainStatus: evaluation.stage1DomainStatus,
          stage1Error: evaluation.stage1Error,
          stage2Status: evaluation.stage2Status,
          stage2Error: evaluation.stage2Error,
          stage3DomainStatus: evaluation.stage3DomainStatus,
          stage6DomainStatus: evaluation.stage6DomainStatus,
          usedPuppeteer: evaluation.usedPuppeteer,
          stage2HtmlPath,
          finalHtmlPath,
          createdAt: new Date().toISOString(),
        };
        fs.writeFileSync(metadataPath, JSON.stringify(metadata, null, 2), "utf8");

        console.log(
          JSON.stringify({
            rawDomain,
            resolvingURL: finalResolvingUrl || "",
            activeSite,
            metadataPath,
            description1,
            description2,
          })
        );
      } catch (error) {
        const fallbackReason = safeString(error?.code || error?.name || "ROW_ERROR");
        console.error(`⚠️ Domain failed but continuing: ${rawDomain} (${fallbackReason})`);
        console.log(
          JSON.stringify({
            rawDomain,
            resolvingURL: "",
            activeSite: true,
            metadataPath: "",
            description1,
            description2,
          })
        );
      }
    }
  } finally {
    if (browser) {
      await browser.close().catch(() => {});
    }
    fs.rmSync(userDataDir, { recursive: true, force: true });
  }
}

main().catch((error) => {
  console.error("Fatal error while enriching resolving URLs:", error);
  process.exit(1);
});
