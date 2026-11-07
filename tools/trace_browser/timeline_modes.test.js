const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { modeSegments } = require('./timeline_modes.js');

test('timeline renderer does not require an external mode global', () => {
  const app = fs.readFileSync(require.resolve('./app.js'), 'utf8');
  assert.doesNotMatch(app, /TraceTimelineModes\.modeSegments/);
});

test('modeSegments merges adjacent samples and closes the final segment', () => {
  const rows = [
    { t: 0, mode: 'Startup' },
    { t: 1.2, mode: 'Startup' },
    { t: 2.4, mode: 'ProbeBW' },
    { t: 4, mode: 'ProbeBW' },
    { t: 5.5, mode: 'SlowDown' }
  ];

  assert.deepEqual(modeSegments(rows, 0, 7), [
    { mode: 'Startup', start: 0, end: 2.4 },
    { mode: 'ProbeBW', start: 2.4, end: 5.5 },
    { mode: 'SlowDown', start: 5.5, end: 7 }
  ]);
});

test('modeSegments splits ProbeBW when app-limited changes', () => {
  const rows = [
    { t: 0, mode: 'ProbeBW', app_limited: false },
    { t: 1, mode: 'ProbeBW', app_limited: true },
    { t: 2, mode: 'ProbeBW', app_limited: true }
  ];

  assert.deepEqual(modeSegments(rows, 0, 3), [
    { mode: 'ProbeBW', app_limited: false, start: 0, end: 1 },
    { mode: 'ProbeBW', app_limited: true, start: 1, end: 3 }
  ]);
});
