#!/usr/bin/env node

import { appendFileSync, mkdirSync, readFileSync } from "node:fs";
import path from "node:path";

const STATUS_ICON = {
  passed: ":white_check_mark:",
  failed: ":x:",
  skipped: ":heavy_minus_sign:",
  pending: ":construction:",
  todo: ":construction:",
};

function outputPath() {
  return process.env.GITHUB_STEP_SUMMARY ?? "test-results/summary.md";
}

function writeSummary(markdown) {
  const file = outputPath();
  mkdirSync(path.dirname(file), { recursive: true });
  appendFileSync(file, markdown);
}

function relativeLocation(filePath) {
  const cwd = process.cwd();
  return filePath.startsWith(cwd) ? path.relative(cwd, filePath) : filePath;
}

function parseVitest(filePath) {
  const report = JSON.parse(readFileSync(filePath, "utf8"));
  const tests = [];

  for (const file of report.testResults ?? []) {
    for (const assertion of file.assertionResults ?? []) {
      const location = assertion.ancestorTitles?.length
        ? `${relativeLocation(file.name)} > ${assertion.ancestorTitles.join(" > ")}`
        : relativeLocation(file.name);

      tests.push({
        name: assertion.title,
        location,
        status: assertion.status,
      });
    }
  }

  return tests;
}

function parseGoTest(filePath) {
  const tests = [];
  const byKey = new Map();

  for (const line of readFileSync(filePath, "utf8").split("\n")) {
    if (!line.trim()) {
      continue;
    }

    let event;
    try {
      event = JSON.parse(line);
    } catch {
      continue;
    }

    if (!event.Test) {
      continue;
    }

    const key = `${event.Package}::${event.Test}`;

    if (event.Action === "run") {
      byKey.set(key, {
        name: event.Test,
        location: event.Package,
        status: "pending",
      });
      continue;
    }

    if (event.Action === "pass") {
      byKey.set(key, {
        name: event.Test,
        location: event.Package,
        status: "passed",
      });
      continue;
    }

    if (event.Action === "fail") {
      byKey.set(key, {
        name: event.Test,
        location: event.Package,
        status: "failed",
      });
      continue;
    }

    if (event.Action === "skip") {
      byKey.set(key, {
        name: event.Test,
        location: event.Package,
        status: "skipped",
      });
    }
  }

  tests.push(
    ...[...byKey.values()].sort((left, right) =>
      `${left.location}::${left.name}`.localeCompare(
        `${right.location}::${right.name}`,
      ),
    ),
  );

  return tests;
}

function detectFormat(filePath) {
  const contents = readFileSync(filePath, "utf8").trim();

  if (!contents) {
    return "empty";
  }

  if (contents.startsWith("{") && contents.includes('"testResults"')) {
    return "vitest";
  }

  return "go";
}

function renderSummary(tests) {
  const passed = tests.filter((test) => test.status === "passed").length;
  const failed = tests.filter((test) => test.status === "failed").length;
  const skipped = tests.filter((test) => test.status === "skipped").length;
  const total = tests.length;

  const lines = ["# Run tests summary", ""];

  if (failed > 0) {
    lines.push(`### :x: **${failed} test${failed === 1 ? "" : "s"} failed**`, "");
  } else {
    lines.push("### :white_check_mark: **All tests passed**", "");
  }

  const summaryParts = [`${passed} test${passed === 1 ? "" : "s"} passed`];
  if (failed > 0) {
    summaryParts.push(`${failed} failed`);
  }
  if (skipped > 0) {
    summaryParts.push(`${skipped} skipped`);
  }

  lines.push(summaryParts.join(" · "), "", "#### Test results:", "");

  for (const test of tests) {
    const icon = STATUS_ICON[test.status] ?? ":grey_question:";
    lines.push(`- ${icon} ${test.name}: ${test.location}`);
  }

  if (total === 0) {
    lines.push("- :grey_question: No tests were reported.");
  }

  lines.push("");
  return `${lines.join("\n")}\n`;
}

function main() {
  const inputPath = process.argv[2];

  if (!inputPath) {
    console.error("Usage: node scripts/github-actions-test-summary.mjs <results.json>");
    process.exit(1);
  }

  const format = detectFormat(inputPath);
  const tests =
    format === "vitest" ? parseVitest(inputPath) : format === "go" ? parseGoTest(inputPath) : [];

  writeSummary(renderSummary(tests));
}

main();
