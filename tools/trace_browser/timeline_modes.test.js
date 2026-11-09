const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const { modeSegments, bandRect, visibleModeSegments, toggleModeSelection } = require('./timeline_modes.js');

test('timeline renderer does not require an external mode global', () => {
  const app = fs.readFileSync(require.resolve('./app.js'), 'utf8');
  assert.doesNotMatch(app, /TraceTimelineModes\.modeSegments/);
});

test('timeline renderer supports multiple selected background modes', () => {
  const app = fs.readFileSync(require.resolve('./app.js'), 'utf8');
  assert.match(app, /selectedModes/);
  assert.match(app, /data-timeline-mode/);
  assert.match(app, /data-timeline-select-all/);
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

test('modeSegments keeps zero-duration transitions for a visible marker', () => {
  const rows = [
    { t: 0, mode: 'Startup' },
    { t: 1, mode: 'ProbeRTT' },
    { t: 1, mode: 'SlowDown' },
    { t: 2, mode: 'SlowDown' }
  ];

  assert.deepEqual(modeSegments(rows, 0, 2), [
    { mode: 'Startup', start: 0, end: 1 },
    { mode: 'ProbeRTT', start: 1, end: 1 },
    { mode: 'SlowDown', start: 1, end: 2 }
  ]);
});

test('bandRect snaps fractional boundaries without leaving background seams', () => {
  assert.deepEqual(bandRect(100.2, 100.8, 64, 700), { x: 100, width: 1 });
  assert.deepEqual(bandRect(100.1, 140.0, 64, 700), { x: 100, width: 40 });
});

test('filtered timeline hides unselected mode transitions', () => {
  const segments = [
    { mode: 'Startup', start: 0, end: 1 },
    { mode: 'SlowDown', start: 1, end: 2 },
    { mode: 'ProbeBW', start: 2, end: 3 },
    { mode: 'ProbeRTT', start: 3, end: 4 }
  ];

  assert.deepEqual(visibleModeSegments(segments, new Set(['Startup'])), [segments[0]]);
});

test('mode selection treats implicit and explicit all-selected states identically', () => {
  const allModes = ['Startup', 'ProbeBW', 'SlowDown', 'ProbeRTT', 'Choked'];

  assert.deepEqual(toggleModeSelection(new Set(), 'Startup', allModes), new Set(['Startup']));
  assert.deepEqual(toggleModeSelection(new Set(allModes), 'Startup', allModes), new Set(['Startup']));
  assert.deepEqual(toggleModeSelection(new Set(['Startup']), 'Startup', allModes), new Set());
});

test('peer rows reserve space for download totals beside long torrent names', () => {
  const styles = fs.readFileSync(require.resolve('./styles.css'), 'utf8');
  assert.match(styles, /\.peer-row\s*\{[^}]*display:\s*grid/);
  assert.match(styles, /grid-template-columns:\s*minmax\(0,\s*1fr\)\s+max-content/);
  assert.match(styles, /\.peer-row\s*>\s*span:first-child\s*\{[^}]*min-width:\s*0/);
  assert.match(styles, /\.peer-id\s*\{[^}]*overflow:\s*hidden/);
  assert.match(styles, /\.peer-value\s*\{[^}]*flex:\s*0\s+0\s+auto/);
});
