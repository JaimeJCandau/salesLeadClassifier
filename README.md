## TOP LINE SUMMARY
Local script to segmenet/classify domains with a combination of Local and AI to optimise cost


## Problem being solved
- Our HubSpot has thousands of "Companies", but they are not classified/segmented 
This script
- Semantically classifies/segments them to define individual sales approach
- Generates a ultra light file with semantics, so that an AI can easily personalise messages (without the cost of parsing each site fully)



## How It Works - TOP LINE
For every domain, it
- Identifies the resolving URL (also used to identify adquisitions, closed companies, etc...)
- Fetches HTML using Pupeteer (hugely cheaper than AI)
- Extract Metas, Menu, Visible text, etc...
- Sends it all in a Prompt to OpenAI to classify

This way: (1) DATA EXTRACTION is done locally (very cheap), and (2) CLASSIFICATION/SEGMENTATION relies on taht with a much cheaper prompt 


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
- `activeSite`
- `redorectedURL`
- `linkedinURL`
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
- `AI_CLASSIFY_MAX_ATTEMPTS=4` (retries per domain on transient OpenAI/parse issues)
- `AI_RETRY_BASE_DELAY_MS=1200` (base backoff between retries)




## REQUIREMENTS
- Local installed npm
- OpenAI API Token
- Time... (I just left it running overnight)

## Run it....
"node runFetchPipeline.js domains.csv 10"
