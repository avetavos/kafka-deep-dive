#!/usr/bin/env node
// Playwright check for <TSPlayground> (in-browser KafkaJS compile-check).
// Modelled on astro-deep-dive's tools/astro-playground.spec.mjs, which
// checks the sibling <AstroPlayground> the same way.
//
// Usage:
//   node tools/ts-playground.spec.mjs <baseUrl>
//   e.g. node tools/ts-playground.spec.mjs http://localhost:4381/kafka
//
// What it checks, per lesson page (EN and TH):
//   1. Navigate to the lesson, wait for the clean playground's Check button.
//   2. Click Check types on the clean producer.send() playground — assert
//      the output panel reports 0 errors ("No errors ✓" / "ไม่มี error").
//   3. Click Check types on the "break it" playground (producer.send({
//      topic: 42, ... })) — assert the diagnostics panel shows a real
//      TypeScript diagnostic (topic: number not assignable to string).
//   4. Assert zero page-level console errors accumulated across the flow.

import { chromium } from 'playwright';

const baseUrl = process.argv[2];
if (!baseUrl) {
  console.error('Usage: node tools/ts-playground.spec.mjs <baseUrl>');
  process.exit(1);
}

const NOT_ASSIGNABLE_TO_STRING = /not assignable to type 'string'/;
const NOT_ASSIGNABLE_TO_STRING_ARRAY = /not assignable to type '\(string \| RegExp\)\[\]'/;

const LESSONS = [
  {
    path: '/en/producers/producer-api/',
    cleanId: 'producers-producer-api',
    brokenId: 'producers-producer-api-broken',
    brokenDiagnostic: NOT_ASSIGNABLE_TO_STRING,
  },
  {
    path: '/th/producers/producer-api/',
    cleanId: 'producers-producer-api',
    brokenId: 'producers-producer-api-broken',
    brokenDiagnostic: NOT_ASSIGNABLE_TO_STRING,
  },
  {
    path: '/en/reading-kafka/verification-tools/',
    cleanId: 'reading-kafka-verification-tools',
    brokenId: 'reading-kafka-verification-tools-broken',
    brokenDiagnostic: NOT_ASSIGNABLE_TO_STRING_ARRAY,
  },
  {
    path: '/th/reading-kafka/verification-tools/',
    cleanId: 'reading-kafka-verification-tools',
    brokenId: 'reading-kafka-verification-tools-broken',
    brokenDiagnostic: NOT_ASSIGNABLE_TO_STRING_ARRAY,
  },
];

async function checkClean(page, id) {
  const root = page.locator(`#${id}`);
  const checkBtn = root.locator('.tsp__check').first();
  await checkBtn.waitFor({ state: 'visible', timeout: 15000 });
  await checkBtn.click();

  const out = root.locator('.tsp__out code').first();
  await out.waitFor({ state: 'visible', timeout: 20000 });
  const text = (await out.textContent()) ?? '';
  if (!/No errors|ไม่มี error/.test(text)) {
    throw new Error(`#${id}: expected a 0-error output, got: ${text.slice(0, 200)}`);
  }
  return text;
}

async function checkBroken(page, id, expectedDiagnostic) {
  const root = page.locator(`#${id}`);
  const checkBtn = root.locator('.tsp__check').first();
  await checkBtn.waitFor({ state: 'visible', timeout: 15000 });
  await checkBtn.click();

  const err = root.locator('.tsp__err code').first();
  await err.waitFor({ state: 'visible', timeout: 20000 });
  const text = (await err.textContent()) ?? '';
  if (!expectedDiagnostic.test(text)) {
    throw new Error(`#${id}: expected a real TS diagnostic (${expectedDiagnostic}), got: ${text.slice(0, 300)}`);
  }
  return text;
}

async function checkLesson(browser, lesson) {
  const page = await browser.newPage();
  const consoleErrors = [];
  page.on('console', (msg) => {
    if (msg.type() === 'error') consoleErrors.push(msg.text());
  });
  page.on('pageerror', (err) => consoleErrors.push(String(err)));

  const url = baseUrl.replace(/\/$/, '') + lesson.path;
  await page.goto(url, { waitUntil: 'load' });

  const cleanOut = await checkClean(page, lesson.cleanId);
  const brokenOut = await checkBroken(page, lesson.brokenId, lesson.brokenDiagnostic);

  if (consoleErrors.length) {
    throw new Error(`${lesson.path}: ${consoleErrors.length} console error(s): ${consoleErrors.join(' | ')}`);
  }

  await page.close();
  return { path: lesson.path, cleanOut, brokenOut };
}

async function main() {
  const browser = await chromium.launch();
  const results = [];
  let failed = false;
  for (const lesson of LESSONS) {
    try {
      const result = await checkLesson(browser, lesson);
      results.push(result);
      console.log(`PASS  ${lesson.path}`);
      console.log(`      clean:  ${result.cleanOut}`);
      console.log(`      broken: ${result.brokenOut.split('\n')[0]}`);
    } catch (err) {
      failed = true;
      console.log(`FAIL  ${lesson.path}`);
      console.log(`      ${err.message}`);
    }
  }
  await browser.close();
  console.log(`\n${results.length}/${LESSONS.length} lesson(s) passed.`);
  process.exit(failed ? 1 : 0);
}

main();
