#!/usr/bin/env -S deno run --allow-net --allow-write

function getVersionForBenchmark(benchmarkName: string): string {
  // by default, return v1.0.0
  const versionMap: Record<string, string> = {
    batch_integration: "v2.0.0",
    label_projection: "v2.0.0",
    // Add more benchmark-specific versions here as needed
  };

  return versionMap[benchmarkName] || "v1.0.0";
}

function unwrapSingleElementArray(jsonString: string): string {
  try {
    const parsed = JSON.parse(jsonString);
    if (Array.isArray(parsed) && parsed.length === 1) {
      return JSON.stringify(parsed[0], null, 2);
    }
    return JSON.stringify(parsed, null, 2);
  } catch {
    return jsonString;
  }
}

function arrayToObject(jsonString: string, keyField: string): string {
  try {
    const parsed = JSON.parse(jsonString);
    if (Array.isArray(parsed)) {
      const obj: Record<string, any> = {};
      for (const item of parsed) {
        if (item && keyField in item) {
          obj[item[keyField]] = item;
        }
      }
      return JSON.stringify(obj, null, 2);
    }
    return JSON.stringify(parsed, null, 2);
  } catch {
    return jsonString;
  }
}

async function extractTaskData(benchmarkName: string, version?: string) {
  const effectiveVersion = version || getVersionForBenchmark(benchmarkName);
  const url = `https://openproblems.bio/benchmarks/${benchmarkName}?version=${effectiveVersion}`;
  console.log(`Fetching: ${url}`);

  const response = await fetch(url);
  const html = await response.text();
  console.log(`HTML length: ${html.length} characters`);

  // Debug: Show a sample of the HTML to understand structure
  const sampleStart =
    html.indexOf("method_id") !== -1 ? html.indexOf("method_id") : 0;
  console.log(
    `HTML sample around method_id:`,
    html.substring(sampleStart, sampleStart + 200),
  );

  // Extract task basic info
  const taskIdMatch = html.match(/taskId:"([^"]*)"/);
  const taskNameMatch = html.match(/taskName:"([^"]*)"/);
  const taskSummaryMatch = html.match(/taskSummary:"([^"]*)"/);
  const taskDescMatch = html.match(/taskDescription:"([^"]*(?:\\.[^"]*)*)"/);

  const taskInfo = {
    taskId: taskIdMatch?.[1] || "",
    taskName: taskNameMatch?.[1] || "",
    taskSummary: taskSummaryMatch?.[1] || "",
    taskDescription:
      taskDescMatch?.[1]?.replace(/\\n/g, "\n").replace(/\\"/g, '"') || "",
  };

  // Simple function to convert JavaScript object syntax to JSON
  function jsToJson(jsString: string): string {
    return (
      jsString
        // Remove $R[X]= assignments
        .replace(/\$R\[\d+\]=/g, "")
        // Replace $R[X] references with null
        .replace(/\$R\[\d+\]/g, "null")
        // Fix boolean values
        .replace(/:\s*!0/g, ": true")
        .replace(/:\s*!1/g, ": false")
        // Quote object keys
        .replace(/([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:/g, '$1"$2":')
        // Clean up trailing commas
        .replace(/,(\s*[}\]])/g, "$1")
    );
  }

  // Extract method data - only include actual method definitions (methods and control_methods)
  // and for v2 pages, deduplicate by method_id
  const isV2 = effectiveVersion.startsWith("v2");
  // Match objects that contain both a task_id of "methods" or "control_methods" AND a method_id
  // Allow one level of nested braces (e.g., references_bibtex:{})
  const methodRegex =
    /\{(?=(?:[^{}]|\{[^{}]*\})*task_id:"(?:methods|control_methods)")(?=(?:[^{}]|\{[^{}]*\})*method_id:"[^"]*")(?:[^{}]|\{[^{}]*\})*\}/g;
  const methodMatches = html.match(methodRegex) || [];
  console.log(
    `Found ${methodMatches.length} method matches:`,
    methodMatches.slice(0, 2),
  );
  const methodsJsonRaw = jsToJson(`[${methodMatches.join(",")}]`);
  let methodsJson = methodsJsonRaw;
  if (isV2) {
    try {
      const parsed = JSON.parse(methodsJsonRaw);
      const deduped = Array.from(
        new Map(parsed.map((m: any) => [m.method_id, m])).values(),
      );
      methodsJson = JSON.stringify(deduped, null, 2);
      console.log(
        `Deduplicated methods (v2): from ${parsed.length} to ${deduped.length}`,
      );
    } catch (e) {
      console.log("Warning: failed to parse methodsJson for deduplication:", e);
    }
  }

  // Extract metric data
  const metricRegex =
    /\{(?=(?:[^{}]|\{[^{}]*\})*task_id:"metrics")(?=(?:[^{}]|\{[^{}]*\})*metric_id:"[^"]*")(?:[^{}]|\{[^{}]*\})*\}/g;
  const metricMatches = html.match(metricRegex) || [];
  console.log(
    `Found ${metricMatches.length} metric matches:`,
    metricMatches.slice(0, 2),
  );
  const metricsJson = jsToJson(`[${metricMatches.join(",")}]`);

  // Extract dataset data
  const datasetRegex =
    /\{(?=(?:[^{}]|\{[^{}]*\})*dataset_id:"[^"]*")(?:[^{}]|\{[^{}]*\})*\}/g;
  const datasetMatches = html.match(datasetRegex) || [];
  console.log(
    `Found ${datasetMatches.length} dataset matches:`,
    datasetMatches.slice(0, 2),
  );
  const datasetsJson = jsToJson(`[${datasetMatches.join(",")}]`);

  // Extract results data (objects with both task_id and method_id)
  const resultRegex =
    /\{(?=(?:[^{}]|\{[^{}]*\})*method_id:"[^"]*")(?=(?:[^{}]|\{[^{}]*\})*resources:\{)(?:[^{}]|\{[^{}]*\})*\}/g;
  const resultMatches = html.match(resultRegex) || [];
  console.log(
    `Found ${resultMatches.length} result matches:`,
    resultMatches.slice(0, 2),
  );
  const resultsJson = jsToJson(`[${resultMatches.join(",")}]`);

  // Create output directory
  const outputDir = `metadata/${benchmarkName}`;
  await Deno.mkdir(outputDir, { recursive: true });

  // Save files
  await Deno.writeTextFile(
    `${outputDir}/task_info.json`,
    JSON.stringify(taskInfo, null, 2),
  );
  await Deno.writeTextFile(
    `${outputDir}/methods.json`,
    arrayToObject(methodsJson, "method_id"),
  );
  await Deno.writeTextFile(
    `${outputDir}/metrics.json`,
    arrayToObject(metricsJson, "metric_id"),
  );
  await Deno.writeTextFile(
    `${outputDir}/datasets.json`,
    arrayToObject(datasetsJson, "dataset_id"),
  );
  await Deno.writeTextFile(
    `${outputDir}/results.json`,
    arrayToObject(resultsJson, "method_id"),
  );

  console.log(`Saved task info to: ${outputDir}/task_info.json`);
  console.log(`Saved methods to: ${outputDir}/methods.json`);
  console.log(`Saved metrics to: ${outputDir}/metrics.json`);
  console.log(`Saved datasets to: ${outputDir}/datasets.json`);
  console.log(`Saved results to: ${outputDir}/results.json`);
}

if (import.meta.main) {
  const benchmarkName = Deno.args[0];
  if (!benchmarkName) {
    console.error(
      "Usage: deno run --allow-net --allow-write extract_task_data.ts <benchmark>",
    );
    Deno.exit(1);
  }

  await extractTaskData(benchmarkName);
}
