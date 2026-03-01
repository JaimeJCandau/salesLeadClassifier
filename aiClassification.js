import fs from "fs";
import path from "path";

const DEFAULT_MODEL = "gpt-5-mini";
const RESULTS_CSV_PATH = path.resolve("results.csv");
const RESULTS_HEADERS = [
  "processed_at_utc",
  "raw_domain",
  "resolving_url",
  "activeSite",
  "redorectedURL",
  "linkedinURL",
  "txt_name",
  "classification",
  "confidence",
  "model",
  "input_chars",
  "status",
  "error_reason",
];
const VALID_CATEGORIES = new Set([
  "Cat_H",
  "Cat_DRR",
  "Cat_A",
  "Cat_YP",
  "Cat_WB",
  "Cat_S",
  "Cat_O",
]);
const CATEGORY_LABELS = {
  Cat_H: "Hoster",
  Cat_DRR: "Domain Registrar/Registry",
  Cat_A: "Agency",
  Cat_YP: "Yellow Pages",
  Cat_WB: "Website Builder",
  Cat_S: "SaaS",
  Cat_O: "Other",
};

const PROMPT_PREFIX = `You classify companies into exactly ONE primary category based ONLY on the data from their website

Only use the category definitions. Do NOT rely on outside knowledge.

Categories:

Cat_H
= Sells or offers Domain registration (retail registrars included) OR ANY hosting (including web, WordPress, Cloud, servers, shared, etc...)

Cat_DRR
= Official domain registry operator managing a TLD infrastructure (e.g., .com, .de). NOT retail registrars. Only choose this if there is explicit evidence they operate a TLD registry.

Cat_A
= Provides website design, WordPress services, development, marketing, SEO, or branding as professional services. Includes any agencies

Cat_YP
= Business directory platform primarily listing companies and monetizing company listings, similar to traditional printed yellow pages.

Cat_WB
= Provides a proprietary platform to build websites using its own software as its core product. May include hosting or domains. Excludes agencies and service providers.

Cat_S
= Software-as-a-service company not primarily focused on hosting, domain registration, website building, agency services, or business listings.

Cat_O
= Everything else that does not clearly fit the above categories.

Rules:
- Choose EXACTLY ONE category.
- Choose the PRIMARY business model.
- Always classify.
- Confidence must reflect certainty between 0.00 and 1.00.
- If insufficient evidence exists, lower the confidence.
- Return results in a JSON with classification and confidence

This is the information you have about the website`;
const DEFAULT_CLASSIFY_MAX_ATTEMPTS = 4;
const DEFAULT_RETRY_BASE_DELAY_MS = 1200;

function safeString(value) {
  return String(value ?? "").trim();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseJsonl(filePath) {
  const raw = fs.readFileSync(filePath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

function toOutputFileName(rawDomain) {
  const base = safeString(rawDomain).replace(/\./g, "").replace(/[^a-zA-Z0-9_-]/g, "");
  return `${base || "unknown"}.txt`;
}

function secondLevelLabelFromHost(hostname) {
  const host = safeString(hostname).toLowerCase().replace(/^www\./, "");
  if (!host) return "";
  const parts = host.split(".").filter(Boolean);
  if (parts.length <= 1) return parts[0] || "";

  const multiPartSuffixes = new Set([
    "co.uk",
    "org.uk",
    "gov.uk",
    "ac.uk",
    "com.au",
    "net.au",
    "org.au",
    "co.nz",
    "com.br",
    "com.mx",
    "co.jp",
    "com.tr",
  ]);

  const suffix2 = `${parts[parts.length - 2]}.${parts[parts.length - 1]}`;
  if (multiPartSuffixes.has(suffix2) && parts.length >= 3) {
    return parts[parts.length - 3];
  }
  return parts[parts.length - 2];
}

function secondLevelLabelFromDomainOrUrl(value) {
  const raw = safeString(value);
  if (!raw) return "";
  try {
    const host = raw.includes("://") ? new URL(raw).hostname : raw.replace(/\/.*$/, "");
    return secondLevelLabelFromHost(host);
  } catch {
    return secondLevelLabelFromHost(raw.replace(/\/.*$/, ""));
  }
}

function normalizeSecondLevelLabel(label) {
  return safeString(label).toLowerCase().replace(/[^a-z0-9]/g, "");
}

function isRedirectedSecondLevel(rawDomain, resolvingUrl) {
  const rawLabel = normalizeSecondLevelLabel(
    secondLevelLabelFromDomainOrUrl(rawDomain)
  );
  const resolvedLabel = normalizeSecondLevelLabel(
    secondLevelLabelFromDomainOrUrl(resolvingUrl)
  );
  if (!rawLabel || !resolvedLabel) return false;
  return rawLabel !== resolvedLabel;
}

function parseDotEnv(content) {
  const out = {};
  const lines = String(content || "").split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eq = trimmed.indexOf("=");
    if (eq <= 0) continue;
    const key = trimmed.slice(0, eq).trim();
    let value = trimmed.slice(eq + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (key) out[key] = value;
  }
  return out;
}

function loadLocalEnv() {
  const envPath = path.resolve(".env");
  if (!fs.existsSync(envPath)) return;
  const parsed = parseDotEnv(fs.readFileSync(envPath, "utf8"));
  for (const [key, value] of Object.entries(parsed)) {
    if (!(key in process.env)) process.env[key] = value;
  }
}

function escapeCsvValue(value) {
  const raw = String(value ?? "");
  if (raw.includes(",") || raw.includes('"') || raw.includes("\n")) {
    return `"${raw.replace(/"/g, '""')}"`;
  }
  return raw;
}

function parseCsvLine(line) {
  const values = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    if (char === '"') {
      const next = line[i + 1];
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (char === "," && !inQuotes) {
      values.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  values.push(current);
  return values;
}

function extractLinkedinUrlFromPayload(payloadText) {
  const text = String(payloadText ?? "");
  if (!text) return "";

  const lines = text.split(/\r?\n/);
  for (const line of lines) {
    const match = line.match(/^\s*linkedinurl\s*:\s*(.*)\s*$/i);
    if (!match) continue;
    return safeString(match[1]);
  }
  return "";
}

function loadExistingResults() {
  if (!fs.existsSync(RESULTS_CSV_PATH)) return [];
  const raw = fs.readFileSync(RESULTS_CSV_PATH, "utf8");
  const lines = raw.split(/\r?\n/).filter(Boolean);
  if (!lines.length) return [];

  const headerColumns = parseCsvLine(lines[0]).map((v) => safeString(v));
  const rawDomainIndex = headerColumns.indexOf("raw_domain");
  if (rawDomainIndex < 0) return [];

  const txtNameIndex = headerColumns.indexOf("txt_name");
  const txtPathIndex = headerColumns.indexOf("txt_path");

  const rows = [];
  for (const line of lines.slice(1)) {
    const columns = parseCsvLine(line);
    const row = {
      processedAtUtc: safeString(columns[headerColumns.indexOf("processed_at_utc")]),
      rawDomain: safeString(columns[rawDomainIndex]),
      resolvingUrl: safeString(columns[headerColumns.indexOf("resolving_url")]),
      activeSite: safeString(columns[headerColumns.indexOf("activeSite")]) || "FALSE",
      redorectedURL: safeString(columns[headerColumns.indexOf("redorectedURL")]) || "FALSE",
      linkedinURL: safeString(columns[headerColumns.indexOf("linkedinURL")]),
      txtName: "",
      classification: safeString(columns[headerColumns.indexOf("classification")]),
      confidence: safeString(columns[headerColumns.indexOf("confidence")]),
      model: safeString(columns[headerColumns.indexOf("model")]),
      inputChars: safeString(columns[headerColumns.indexOf("input_chars")]),
      status: safeString(columns[headerColumns.indexOf("status")]),
      errorReason: safeString(columns[headerColumns.indexOf("error_reason")]),
    };
    if (txtNameIndex >= 0) row.txtName = safeString(columns[txtNameIndex]);
    else if (txtPathIndex >= 0) row.txtName = path.basename(safeString(columns[txtPathIndex]));
    if (row.rawDomain) rows.push(row);
  }
  return dedupeRowsByRawDomain(rows);
}

function dedupeRowsByRawDomain(rows) {
  const byDomain = new Map();
  for (const row of rows) {
    const key = safeString(row.rawDomain).toLowerCase();
    if (!key) continue;
    byDomain.set(key, row);
  }
  return Array.from(byDomain.values());
}

function writeResults(rows) {
  const lines = [RESULTS_HEADERS.join(",")];
  for (const row of rows) {
    const fields = [
      row.processedAtUtc,
      row.rawDomain,
      row.resolvingUrl,
      row.activeSite,
      row.redorectedURL,
      row.linkedinURL,
      row.txtName,
      row.classification,
      row.confidence,
      row.model,
      row.inputChars,
      row.status,
      row.errorReason,
    ];
    lines.push(fields.map(escapeCsvValue).join(","));
  }
  fs.writeFileSync(RESULTS_CSV_PATH, `${lines.join("\n")}\n`, "utf8");
}

function upsertResultRow(existingRows, row) {
  const filtered = existingRows.filter(
    (current) => safeString(current.rawDomain).toLowerCase() !== safeString(row.rawDomain).toLowerCase()
  );
  filtered.push(row);
  return filtered;
}

function extractResponseText(payload) {
  if (typeof payload?.output_text === "string" && payload.output_text.trim()) {
    return payload.output_text.trim();
  }
  if (Array.isArray(payload?.output_text)) {
    const joined = payload.output_text
      .map((part) => safeString(part))
      .filter(Boolean)
      .join("\n")
      .trim();
    if (joined) return joined;
  }

  // Some models return already-parsed structured outputs.
  if (payload?.output_parsed && typeof payload.output_parsed === "object") {
    return JSON.stringify(payload.output_parsed);
  }

  const output = Array.isArray(payload?.output) ? payload.output : [];
  const parts = [];
  for (const item of output) {
    if (typeof item?.output_text === "string" && safeString(item.output_text)) {
      parts.push(safeString(item.output_text));
    }

    const content = Array.isArray(item?.content) ? item.content : [];
    for (const part of content) {
      if (part?.parsed && typeof part.parsed === "object") {
        return JSON.stringify(part.parsed);
      }
      if (part?.json && typeof part.json === "object") {
        return JSON.stringify(part.json);
      }
      if (typeof part?.text === "string" && part.text.trim()) {
        parts.push(part.text.trim());
      }
    }
  }
  return parts.join("\n").trim();
}

function parseJsonFromText(text) {
  const trimmed = safeString(text)
    .replace(/^```(?:json)?\s*/i, "")
    .replace(/\s*```$/, "")
    .trim();
  if (!trimmed) throw new Error("empty_model_response");

  try {
    return JSON.parse(trimmed);
  } catch {
    const firstBrace = trimmed.indexOf("{");
    const lastBrace = trimmed.lastIndexOf("}");
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      try {
        return JSON.parse(trimmed.slice(firstBrace, lastBrace + 1));
      } catch {
        // Continue with regex fallback.
      }
    }

    const cls =
      trimmed.match(/["']?classification["']?\s*[:=]\s*["']?([A-Za-z_]+)["']?/i)?.[1] || "";
    const confRaw =
      trimmed.match(/["']?confidence["']?\s*[:=]\s*([0-9]*\.?[0-9]+)/i)?.[1] || "";
    const conf = Number(confRaw);

    if (cls && Number.isFinite(conf)) {
      return { classification: cls, confidence: conf };
    }

    throw new Error("invalid_json_response");
  }
}

function parseHttpStatusFromError(message) {
  const match = safeString(message).match(/^openai_http_(\d+):/);
  return match ? Number(match[1]) : null;
}

function isRetryableStatus(status) {
  if (!Number.isFinite(status)) return false;
  return status === 408 || status === 409 || status === 429 || status >= 500;
}

function isRetryableClassificationError(error) {
  const message = safeString(error?.message || error).toLowerCase();
  const status = parseHttpStatusFromError(message);

  if (Number.isFinite(status) && isRetryableStatus(status)) return true;
  if (message.includes("empty_model_response")) return true;
  if (message.includes("invalid_json_response")) return true;
  if (message.includes("fetch failed")) return true;
  if (message.includes("etimedout")) return true;
  if (message.includes("econnreset")) return true;
  if (message.includes("socket hang up")) return true;
  return false;
}

function getRetryDelayMs(attempt, baseDelayMs) {
  const jitter = Math.floor(Math.random() * 350);
  return baseDelayMs * Math.max(1, attempt) + jitter;
}

function normalizeClassification(result) {
  const classification = safeString(result?.classification);
  const confidenceRaw = result?.confidence;
  const confidenceNum = Number(confidenceRaw);
  if (!VALID_CATEGORIES.has(classification)) {
    throw new Error(`invalid_classification:${classification || "empty"}`);
  }
  if (!Number.isFinite(confidenceNum) || confidenceNum < 0 || confidenceNum > 1) {
    throw new Error(`invalid_confidence:${String(confidenceRaw)}`);
  }
  return {
    classification: CATEGORY_LABELS[classification] || classification,
    confidence: confidenceNum.toFixed(4),
  };
}

async function classifyWithOpenAI({ apiKey, model, promptText }) {
  const makeRequestBody = (suffix = "") => ({
    model,
    max_output_tokens: 400,
    text: {
      format: {
        type: "json_schema",
        name: "classification_result",
        schema: {
          type: "object",
          additionalProperties: false,
          properties: {
            classification: {
              type: "string",
              enum: Array.from(VALID_CATEGORIES),
            },
            confidence: {
              type: "number",
              minimum: 0,
              maximum: 1,
            },
          },
          required: ["classification", "confidence"],
        },
        strict: true,
      },
    },
    input: [
      {
        role: "system",
        content: [
          {
            type: "input_text",
            text: "Return valid JSON only. No markdown. No extra keys.",
          },
        ],
      },
      {
        role: "user",
        content: [
          {
            type: "input_text",
            text: `${promptText}${suffix}`,
          },
        ],
      },
    ],
  });

  for (let pass = 1; pass <= 2; pass += 1) {
    const suffix =
      pass === 1
        ? ""
        : "\n\nIMPORTANT: respond with raw JSON object only. No markdown, no prose.";

    const response = await fetch("https://api.openai.com/v1/responses", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify(makeRequestBody(suffix)),
    });

    if (!response.ok) {
      const errorBody = await response.text();
      throw new Error(`openai_http_${response.status}:${safeString(errorBody).slice(0, 300)}`);
    }

    const payload = await response.json();
    const outputText = extractResponseText(payload);
    if (!safeString(outputText)) {
      if (pass < 2) continue;
      throw new Error("empty_model_response");
    }

    const parsed = parseJsonFromText(outputText);
    return normalizeClassification(parsed);
  }

  throw new Error("empty_model_response");
}

async function main() {
  const inputFile = process.argv[2];
  const limitArg = Number(process.argv[3]);
  const rowLimit = Number.isFinite(limitArg) && limitArg > 0 ? limitArg : null;

  if (!inputFile) {
    console.error("Usage: node aiClassification.js <input-jsonl-file> [limit]");
    process.exit(1);
  }

  const absPath = path.resolve(inputFile);
  if (!fs.existsSync(absPath)) {
    console.error(`Input file not found: ${absPath}`);
    process.exit(1);
  }

  loadLocalEnv();
  const apiKey = safeString(process.env.OPENAI_API_KEY);
  const model = safeString(process.env.OPENAI_MODEL) || DEFAULT_MODEL;
  const classifyMaxAttempts = Math.max(
    1,
    Number.parseInt(
      safeString(process.env.AI_CLASSIFY_MAX_ATTEMPTS) || String(DEFAULT_CLASSIFY_MAX_ATTEMPTS),
      10
    ) || DEFAULT_CLASSIFY_MAX_ATTEMPTS
  );
  const retryBaseDelayMs = Math.max(
    250,
    Number.parseInt(
      safeString(process.env.AI_RETRY_BASE_DELAY_MS) || String(DEFAULT_RETRY_BASE_DELAY_MS),
      10
    ) || DEFAULT_RETRY_BASE_DELAY_MS
  );

  if (!apiKey) {
    console.error("Missing OPENAI_API_KEY in environment or .env");
    process.exit(1);
  }

  const rows = parseJsonl(absPath);
  const targets = rowLimit ? rows.slice(0, rowLimit) : rows;
  let resultsRows = loadExistingResults();

  let okCount = 0;
  let skippedCount = 0;
  let errorCount = 0;

  for (const row of targets) {
    const processedAtUtc = new Date().toISOString();
    const rawDomain = safeString(row?.rawDomain);
    const resolvingUrl = safeString(row?.resolvingURL);
    const description1 = safeString(row?.description1);
    const description2 = safeString(row?.description2);
    const hasDescriptionContent = !!(description1 || description2);
    const activeSiteValue = row?.activeSite === true ? "TRUE" : "FALSE";
    const redorectedURL = isRedirectedSecondLevel(rawDomain, resolvingUrl) ? "TRUE" : "FALSE";
    const txtName = toOutputFileName(rawDomain);
    const txtPath = path.resolve("output", txtName);

    if (!(row?.activeSite === true || hasDescriptionContent)) {
      resultsRows = upsertResultRow(resultsRows, {
        processedAtUtc,
        rawDomain,
        resolvingUrl,
        activeSite: activeSiteValue,
        redorectedURL,
        linkedinURL: "",
        txtName,
        classification: "",
        confidence: "",
        model,
        inputChars: 0,
        status: "skipped",
        errorReason: "inactive_site",
      });
      skippedCount += 1;
      continue;
    }

    if (!fs.existsSync(txtPath)) {
      resultsRows = upsertResultRow(resultsRows, {
        processedAtUtc,
        rawDomain,
        resolvingUrl,
        activeSite: activeSiteValue,
        redorectedURL,
        linkedinURL: "",
        txtName,
        classification: "",
        confidence: "",
        model,
        inputChars: 0,
        status: "skipped",
        errorReason: "missing_txt",
      });
      skippedCount += 1;
      continue;
    }

    const extractedText = safeString(fs.readFileSync(txtPath, "utf8"));
    if (!extractedText) {
      resultsRows = upsertResultRow(resultsRows, {
        processedAtUtc,
        rawDomain,
        resolvingUrl,
        activeSite: activeSiteValue,
        redorectedURL,
        linkedinURL: "",
        txtName,
        classification: "",
        confidence: "",
        model,
        inputChars: 0,
        status: "skipped",
        errorReason: "empty_txt",
      });
      skippedCount += 1;
      continue;
    }

    const promptText = `${PROMPT_PREFIX}\n\n${extractedText}`;
    const linkedinURL = extractLinkedinUrlFromPayload(extractedText);

    try {
      let result = null;
      let lastError = null;

      for (let attempt = 1; attempt <= classifyMaxAttempts; attempt += 1) {
        try {
          result = await classifyWithOpenAI({
            apiKey,
            model,
            promptText,
          });
          lastError = null;
          break;
        } catch (error) {
          lastError = error;
          const canRetry =
            attempt < classifyMaxAttempts && isRetryableClassificationError(error);
          if (!canRetry) break;

          const waitMs = getRetryDelayMs(attempt, retryBaseDelayMs);
          console.warn(
            `Retrying AI call (${attempt}/${classifyMaxAttempts}) for ${rawDomain} after error: ${safeString(
              error?.message || error
            )} [wait ${waitMs}ms]`
          );
          await sleep(waitMs);
        }
      }

      if (!result) {
        throw lastError || new Error("classification_failed");
      }

      resultsRows = upsertResultRow(resultsRows, {
        processedAtUtc,
        rawDomain,
        resolvingUrl,
        activeSite: activeSiteValue,
        redorectedURL,
        linkedinURL,
        txtName,
        classification: result.classification,
        confidence: result.confidence,
        model,
        inputChars: extractedText.length,
        status: "ok",
        errorReason: "",
      });
      okCount += 1;
    } catch (error) {
      resultsRows = upsertResultRow(resultsRows, {
        processedAtUtc,
        rawDomain,
        resolvingUrl,
        activeSite: activeSiteValue,
        redorectedURL,
        linkedinURL,
        txtName,
        classification: "",
        confidence: "",
        model,
        inputChars: extractedText.length,
        status: "error",
        errorReason: safeString(error?.message || error).slice(0, 500),
      });
      errorCount += 1;
    }
  }
  writeResults(resultsRows);

  console.log(`Rows received: ${rows.length}`);
  console.log(`Rows attempted for AI: ${targets.length}`);
  console.log(`AI ok: ${okCount}`);
  console.log(`AI skipped: ${skippedCount}`);
  console.log(`AI errors: ${errorCount}`);
  console.log(`results.csv path: ${RESULTS_CSV_PATH}`);
}

main().catch((error) => {
  console.error("aiClassification.js failed:", error);
  process.exit(1);
});
