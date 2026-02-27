import fs from "fs";
import path from "path";
import { spawn } from "child_process";

function ensureOutputDir() {
  const dir = path.resolve("output");
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  return dir;
}

function nowStamp() {
  const d = new Date();
  const p = (n) => String(n).padStart(2, "0");
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}_${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
}

function parseFetchLine(line) {
  try {
    const parsedJson = JSON.parse(line);
    if (parsedJson && typeof parsedJson === "object" && safeString(parsedJson.rawDomain)) {
      return {
        rawDomain: safeString(parsedJson.rawDomain),
        resolvingURL: safeString(parsedJson.resolvingURL) || null,
        activeSite: parsedJson.activeSite === true,
        metadataPath: safeString(parsedJson.metadataPath) || null,
        description1: safeString(parsedJson.description1),
        description2: safeString(parsedJson.description2),
      };
    }
  } catch {
    // Not a JSON row, continue with CSV compatibility parser.
  }

  // Expected format from fetchURL.js:
  // rawDomain,resolvingURL,activeSite,metadataPath
  const parts = line.split(",");
  if (parts.length < 3) return null;

  const activeSiteRaw = parts[parts.length - 1].trim().toLowerCase();
  let metadataPath = null;
  let resolvingURL = null;
  let rawDomain = null;
  let activeFlag = null;

  // New format with metadata path
  if (parts.length >= 4) {
    const maybeActive = parts[parts.length - 2].trim().toLowerCase();
    if (maybeActive === "true" || maybeActive === "false") {
      metadataPath = parts[parts.length - 1].trim() || null;
      activeFlag = maybeActive === "true";
      resolvingURL = parts[parts.length - 3].trim();
      rawDomain = parts.slice(0, parts.length - 3).join(",").trim();
    }
  }

  // Backward compatibility with old 3-field output
  if (activeFlag === null) {
    if (activeSiteRaw !== "true" && activeSiteRaw !== "false") return null;
    activeFlag = activeSiteRaw === "true";
    resolvingURL = parts[parts.length - 2].trim();
    rawDomain = parts.slice(0, parts.length - 2).join(",").trim();
  }

  if (!rawDomain) return null;

  return {
    rawDomain,
    resolvingURL: resolvingURL || null,
    activeSite: activeFlag,
    metadataPath: metadataPath || null,
    description1: "",
    description2: "",
  };
}

function safeString(value) {
  return String(value ?? "").trim();
}

async function runFetchAndCollect(csvPath, limitArg, outputJsonlPath) {
  return new Promise((resolve, reject) => {
    const args = ["fetchURL.js", csvPath];
    if (limitArg) args.push(String(limitArg));

    const child = spawn("node", args, { stdio: ["ignore", "pipe", "pipe"] });

    let stdoutBuffer = "";
    let stderrBuffer = "";
    const collected = [];

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString("utf8");
      process.stdout.write(text); // keep live logs visible
      stdoutBuffer += text;

      const lines = stdoutBuffer.split("\n");
      stdoutBuffer = lines.pop() || "";

      for (const line of lines) {
        const parsed = parseFetchLine(line.trim());
        if (parsed) collected.push(parsed);
      }
    });

    child.stderr.on("data", (chunk) => {
      const text = chunk.toString("utf8");
      process.stderr.write(text);
      stderrBuffer += text;
    });

    child.on("close", (code) => {
      if (stdoutBuffer.trim()) {
        const parsed = parseFetchLine(stdoutBuffer.trim());
        if (parsed) collected.push(parsed);
      }

      if (code !== 0) {
        reject(
          new Error(`fetchURL.js exited with code ${code}. stderr:\n${stderrBuffer}`)
        );
        return;
      }

      const jsonl = collected.map((r) => JSON.stringify(r)).join("\n");
      fs.writeFileSync(outputJsonlPath, jsonl + (jsonl ? "\n" : ""), "utf8");
      resolve(collected.length);
    });

    child.on("error", reject);
  });
}

async function runSecondStep(inputJsonlPath) {
  return new Promise((resolve, reject) => {
    const child = spawn("node", ["processResolvedDomains.js", inputJsonlPath], {
      stdio: "inherit",
    });

    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`processResolvedDomains.js exited with code ${code}`));
        return;
      }
      resolve();
    });

    child.on("error", reject);
  });
}

async function runThirdStep(inputJsonlPath, limitArg) {
  return new Promise((resolve, reject) => {
    const args = ["aiClassification.js", inputJsonlPath];
    if (limitArg) args.push(String(limitArg));

    const child = spawn("node", args, { stdio: "inherit" });

    child.on("close", (code) => {
      if (code !== 0) {
        reject(new Error(`aiClassification.js exited with code ${code}`));
        return;
      }
      resolve();
    });

    child.on("error", reject);
  });
}

async function main() {
  const csvPath = process.argv[2] || "domains.csv";
  const limitArg = process.argv[3] || null;

  const outputDir = ensureOutputDir();
  const outputJsonlPath = path.join(
    outputDir,
    `fetch_results_${nowStamp()}.jsonl`
  );

  console.log(`Running fetchURL.js with CSV: ${csvPath}`);
  const rowCount = await runFetchAndCollect(csvPath, limitArg, outputJsonlPath);
  console.log(`Captured ${rowCount} rows into: ${outputJsonlPath}`);

  console.log("Triggering second step: processResolvedDomains.js");
  await runSecondStep(outputJsonlPath);

  console.log("Triggering third step: aiClassification.js");
  await runThirdStep(outputJsonlPath, limitArg);
}

main().catch((error) => {
  console.error("runFetchPipeline.js failed:", error.message || error);
  process.exit(1);
});
