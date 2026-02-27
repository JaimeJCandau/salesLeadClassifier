## Problem being solved
Sales team has a massive set of potential leads
This script
- Semantically classifies to define sales approach
- Generates a ultra light file with semantics, so that an AI can easily personalise messages (without the cost of parsing each site fully)



## How It Works - TOP LINE
For every domain, it
- Identifies the resolving URL
- Fetches HTML using Pupeteer
- Extract Metas, Menu, Visible text, etc...
- Sends it all in a Prompt to OpenAI to classify
- NOTE: The extraction is done locally (very cheap), and the AI is fed with that. This is hugely cheaper


## How It Works - HOW SCRIPTS ARE ORCHESTRATED
1. `runFetchPipeline.js` runs `fetchURL.js`.
2. It captures each output row from `fetchURL.js` in this format:
   - `rawDomain,resolvingURL,activeSite,metadataPath`
3. It writes those rows to a timestamped JSONL file:
   - `output/fetch_results_YYYYMMDD_HHMMSS.jsonl`
4. During fetch, `fetchURL.js` also saves local artifacts per domain:
   - `output/fetch_artifacts/<domain>_stage2.html`
   - `output/fetch_artifacts/<domain>_final.html`
   - `output/fetch_artifacts/<domain>.json`
5. It triggers `processResolvedDomains.js` and passes that JSONL file path as input.
6. It triggers `aiClassification.js` with the same JSONL file.

`processResolvedDomains.js` reads the local artifact paths from `metadataPath` and works from local HTML (no second website fetch).
It writes one compact, token-optimized payload file per active domain in `output/`:
- `<rawDomainWithoutDots>.txt`
- Format:
  - `metaTitle:`
  - `metaDescription:`
  - `h1AndH2:`
  - `visibleText:`
  - `menuValues:`
  - `linkedInURL:`

`aiClassification.js` then reads those `output/*.txt` payloads and calls OpenAI for classification.
Results in root `results.csv` are upserted by `raw_domain` (latest run replaces older row for that same domain) with these columns:
- `processed_at_utc`
- `raw_domain`
- `resolving_url`
- `txt_name`
- `classification`
- `confidence`
- `model`
- `input_chars`
- `status` (`ok`, `error`, `skipped`)
- `error_reason`

Required environment variables in `.env`:
- `OPENAI_API_KEY=<your_api_key>`

Optional:
- `OPENAI_MODEL=gpt-5-mini`

This was tested end-to-end with 2 rows and worked correctly.

## Setup

```bash
npm install
cp .env.example .env
```

## Run Overnight

```bash
node runFetchPipeline.js domains.csv
```

## Optional Quick Test

```bash
node runFetchPipeline.js domains.csv 10
```

The optional second argument is a row limit.

## Run AI Step Only

```bash
node aiClassification.js output/fetch_results_YYYYMMDD_HHMMSS.jsonl
```
