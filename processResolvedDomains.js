import fs from "fs";
import path from "path";
import { load } from "cheerio";

function parseJsonl(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function safeString(value) {
  return String(value ?? "").trim();
}

function normalizeWhitespace(value) {
  return safeString(value).replace(/\s+/g, " ").trim();
}

function toOutputFileName(rawDomain) {
  // Token-optimized payload file: example.com -> examplecom.txt
  const base = safeString(rawDomain).replace(/\./g, "").replace(/[^a-zA-Z0-9_-]/g, "");
  return `${base || "unknown"}.txt`;
}

function limitText(value, maxChars = 250) {
  return normalizeWhitespace(value).slice(0, maxChars);
}

function limitArrayByTotalChars(values, maxChars = 250) {
  const out = [];
  let used = 0;

  for (const raw of values) {
    const item = normalizeWhitespace(raw);
    if (!item) continue;

    const separatorCost = out.length > 0 ? 1 : 0;
    const remaining = maxChars - used - separatorCost;
    if (remaining <= 0) break;

    if (item.length <= remaining) {
      out.push(item);
      used += separatorCost + item.length;
      continue;
    }

    out.push(item.slice(0, remaining));
    used = maxChars;
    break;
  }

  return out;
}

function parseHtmlContent(html) {
  const $ = load(typeof html === "string" ? html : "");

  const metaTitle =
    normalizeWhitespace($("title").first().text()) ||
    normalizeWhitespace($('meta[property="og:title"]').attr("content")) ||
    "";

  const metaDescription =
    limitText($('meta[name="description"]').attr("content"), 800) ||
    limitText($('meta[property="og:description"]').attr("content"), 800) ||
    limitText($('meta[name="twitter:description"]').attr("content"), 800) ||
    "";

  const h1Values = $("h1")
    .map((_, el) => normalizeWhitespace($(el).text()))
    .get()
    .filter(Boolean);

  const h2Values = $("h2")
    .map((_, el) => normalizeWhitespace($(el).text()))
    .get()
    .filter(Boolean);

  const h1AndH2 = limitArrayByTotalChars(
    Array.from(new Set([...h1Values, ...h2Values])),
    250
  );

  const menuLinks = [];
  const seenMenuLinks = new Set();
  const navLikeSelector =
    "nav, header nav, [role='navigation'], .menu, #menu, [class*='menu'], [id*='menu'], .navbar, #navbar, [class*='nav-'], [id*='nav-']";

  $(navLikeSelector)
    .find("a[href]")
    .each((_, el) => {
      const href = safeString($(el).attr("href"));
      if (!href) return;
      if (href.startsWith("mailto:") || href.startsWith("tel:")) return;
      if (href.startsWith("#")) return;

      let absolute = null;
      try {
        absolute = new URL(href, "https://placeholder.local").toString();
      } catch {
        return;
      }

      const u = new URL(absolute);
      const isInternal =
        u.hostname === "placeholder.local" ||
        u.hostname === "www.placeholder.local";
      if (!isInternal) return;

      const normalizedLink = `${u.pathname}${u.search}` || "/";
      if (seenMenuLinks.has(normalizedLink)) return;
      seenMenuLinks.add(normalizedLink);
      menuLinks.push(normalizedLink);
    });

  // Remove menu/nav/footer/header + h1/h2 before collecting visible text.
  const $bodyClone = $("body").clone();
  $bodyClone.find("script,style,noscript,svg,footer,header,nav,aside,form").remove();
  $bodyClone.find("h1,h2").remove();
  $bodyClone.find(navLikeSelector).remove();

  const visibleText = limitText($bodyClone.text(), 250);

  const linkedInCandidates = [];
  $("a[href*='linkedin.com']").each((_, el) => {
    const href = safeString($(el).attr("href"));
    if (!href) return;
    try {
      const u = new URL(href, "https://placeholder.local");
      const host = u.hostname.toLowerCase();
      if (!host.includes("linkedin.com")) return;
      u.hash = "";
      u.search = "";
      linkedInCandidates.push(u.toString().replace(/\/$/, ""));
    } catch {
      // ignore malformed href
    }
  });

  const uniqueLinkedIn = Array.from(new Set(linkedInCandidates));

  const isGenericLinkedIn = (url) => {
    try {
      const u = new URL(url);
      const path = u.pathname.replace(/\/+$/, "");
      if (!path || path === "/") return true;

      const segments = path.split("/").filter(Boolean);
      if (!segments.length) return true;

      const first = segments[0].toLowerCase();
      const second = (segments[1] || "").toLowerCase();

      // Generic hubs and non-profile landing pages.
      if (
        [
          "feed",
          "jobs",
          "learning",
          "help",
          "company",
          "school",
          "in",
          "showcase",
          "groups",
          "pulse",
          "sales",
          "events",
        ].includes(first) &&
        !second
      ) {
        return true;
      }

      return false;
    } catch {
      return true;
    }
  };

  const linkedInPriority = (url) => {
    const path = new URL(url).pathname.toLowerCase();
    if (path.startsWith("/company/")) return 1;
    if (path.startsWith("/school/")) return 2;
    if (path.startsWith("/showcase/")) return 3;
    if (path.startsWith("/in/")) return 4;
    if (path.startsWith("/pub/")) return 5;
    if (path.startsWith("/groups/")) return 6;
    return 99;
  };

  const specificLinkedIn = uniqueLinkedIn.filter((u) => !isGenericLinkedIn(u));
  specificLinkedIn.sort((a, b) => linkedInPriority(a) - linkedInPriority(b));
  const linkedInURL = specificLinkedIn[0] || "";
  const menuValues = limitArrayByTotalChars(menuLinks, 250);

  return {
    metaTitle,
    metaDescription,
    h1AndH2,
    visibleText,
    menuValues,
    linkedInURL,
  };
}

function buildCompactPayload(extracted) {
  const h = Array.isArray(extracted?.h1AndH2) ? extracted.h1AndH2.join(" | ") : "";
  const m = Array.isArray(extracted?.menuValues) ? extracted.menuValues.join(" | ") : "";
  const hasSiteMeta =
    !!safeString(extracted?.metaTitle) && !!safeString(extracted?.metaDescription);

  // Use full variable names so each API call is self-describing.
  const lines = [
    `metaTitle:${safeString(extracted?.metaTitle)}`,
    `metaDescription:${safeString(extracted?.metaDescription)}`,
    `h1AndH2:${safeString(h)}`,
    `visibleText:${safeString(extracted?.visibleText)}`,
    `menuValues:${safeString(m)}`,
    `linkedInURL:${safeString(extracted?.linkedInURL)}`,
  ];

  if (!hasSiteMeta) {
    lines.push(`description1:${safeString(extracted?.description1)}`);
    lines.push(`description2:${safeString(extracted?.description2)}`);
  }

  return lines.join("\n");
}

async function fetchHtmlFallback(urlOrDomain) {
  const raw = safeString(urlOrDomain);
  if (!raw) return "";

  let url = raw;
  if (!/^https?:\/\//i.test(url)) {
    url = `https://${url}`;
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);
  try {
    const response = await fetch(url, {
      method: "GET",
      redirect: "follow",
      signal: controller.signal,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
    });
    const text = await response.text();
    return safeString(text) ? text : "";
  } catch {
    return "";
  } finally {
    clearTimeout(timeout);
  }
}

async function main() {
  const inputFile = process.argv[2];
  if (!inputFile) {
    console.error("Usage: node processResolvedDomains.js <input-jsonl-file>");
    process.exit(1);
  }

  const absPath = path.resolve(inputFile);
  if (!fs.existsSync(absPath)) {
    console.error(`Input file not found: ${absPath}`);
    process.exit(1);
  }

  const rows = parseJsonl(absPath);
  const outputDir = path.resolve("output");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  let processed = 0;
  let skippedInactive = 0;
  let skippedMissingMetadata = 0;
  let skippedMissingHtml = 0;
  let processedDescriptionsOnly = 0;
  let processedFallbackFetch = 0;

  for (const row of rows) {
    try {
      const rowDescription1 = safeString(row?.description1);
      const rowDescription2 = safeString(row?.description2);
      const hasDescriptionContent = !!(rowDescription1 || rowDescription2);
      const shouldProcess = row?.activeSite === true || hasDescriptionContent;
      if (!shouldProcess) {
        skippedInactive += 1;
        continue;
      }

      const metadataPath = row?.metadataPath ? path.resolve(row.metadataPath) : null;
      let metadata = null;
      if (metadataPath && fs.existsSync(metadataPath)) {
        metadata = JSON.parse(fs.readFileSync(metadataPath, "utf8"));
      }
      const finalHtmlPath = metadata?.finalHtmlPath ? path.resolve(metadata.finalHtmlPath) : null;
      const stage2HtmlPath = metadata?.stage2HtmlPath ? path.resolve(metadata.stage2HtmlPath) : null;
      const rawDomain = safeString(metadata?.rawDomain || row?.rawDomain);
      const resolvingUrl = safeString(metadata?.resolvingURL || row?.resolvingURL);
      const description1 = safeString(metadata?.description1 || rowDescription1);
      const description2 = safeString(metadata?.description2 || rowDescription2);
      let html = "";
      if (finalHtmlPath && fs.existsSync(finalHtmlPath)) {
        html = fs.readFileSync(finalHtmlPath, "utf8");
      } else if (stage2HtmlPath && fs.existsSync(stage2HtmlPath)) {
        html = fs.readFileSync(stage2HtmlPath, "utf8");
      }

      if (!safeString(html) && (resolvingUrl || rawDomain)) {
        html = await fetchHtmlFallback(resolvingUrl || rawDomain);
        if (safeString(html)) processedFallbackFetch += 1;
      }

      const extracted = safeString(html) ? parseHtmlContent(html) : {};
      if (!safeString(html) && !hasDescriptionContent) {
        if (!metadataPath || !fs.existsSync(metadataPath)) skippedMissingMetadata += 1;
        skippedMissingHtml += 1;
        continue;
      }

      if (!safeString(html) && hasDescriptionContent) {
        processedDescriptionsOnly += 1;
      } else {
        processed += 1;
      }

      const outputFileName = toOutputFileName(rawDomain);
      const outputPath = path.join(outputDir, outputFileName);
      fs.writeFileSync(
        outputPath,
        buildCompactPayload({
          ...extracted,
          description1,
          description2,
        }),
        "utf8"
      );
    } catch {
      // Ignore malformed metadata rows and continue.
      skippedMissingMetadata += 1;
    }
  }

  console.log(`Rows received: ${rows.length}`);
  console.log(`Processed (HTML extracted): ${processed}`);
  console.log(`Processed (fallback live fetch): ${processedFallbackFetch}`);
  console.log(`Processed (descriptions only): ${processedDescriptionsOnly}`);
  console.log(`Skipped inactive (activeSite=false and no descriptions): ${skippedInactive}`);
  console.log(`Skipped missing metadata: ${skippedMissingMetadata}`);
  console.log(`Skipped missing HTML: ${skippedMissingHtml}`);
}

main().catch((error) => {
  console.error("processResolvedDomains.js failed:", error);
  process.exit(1);
});
