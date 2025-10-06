#!/usr/bin/env -S deno run --allow-net --allow-write

async function extractTaskData(
  benchmarkName: string,
  version: string = "v1.0.0",
) {
  const url = `https://openproblems.bio/benchmarks/${benchmarkName}?version=${version}`;

  const response = await fetch(url);
  const html = await response.text();

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

  // Extract method data
  const methodRegex = /\{[^{}]*method_id:"[^"]*"[^{}]*\}/g;
  const methodMatches = html.match(methodRegex) || [];
  const methodsJson = jsToJson(`[${methodMatches.join(",")}]`);

  // Extract metric data
  const metricRegex = /\{[^{}]*metric_id:"[^"]*"[^{}]*\}/g;
  const metricMatches = html.match(metricRegex) || [];
  const metricsJson = jsToJson(`[${metricMatches.join(",")}]`);

  // Extract dataset data
  const datasetRegex = /\{[^{}]*dataset_id:"[^"]*"[^{}]*\}/g;
  const datasetMatches = html.match(datasetRegex) || [];
  const datasetsJson = jsToJson(`[${datasetMatches.join(",")}]`);

  // Extract results data (objects with both task_id and method_id)
  const resultRegex = /\{[^{}]*task_id:"[^"]*"[^{}]*method_id:"[^"]*"[^{}]*\}/g;
  const resultMatches = html.match(resultRegex) || [];
  const resultsJson = jsToJson(`[${resultMatches.join(",")}]`);

  // Create output directory
  const outputDir = `data/${benchmarkName}`;
  await Deno.mkdir(outputDir, { recursive: true });

  // Save files
  await Deno.writeTextFile(
    `${outputDir}/task_info.json`,
    JSON.stringify(taskInfo, null, 2),
  );
  await Deno.writeTextFile(`${outputDir}/methods.json`, methodsJson);
  await Deno.writeTextFile(`${outputDir}/metrics.json`, metricsJson);
  await Deno.writeTextFile(`${outputDir}/datasets.json`, datasetsJson);
  await Deno.writeTextFile(`${outputDir}/results.json`, resultsJson);

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
