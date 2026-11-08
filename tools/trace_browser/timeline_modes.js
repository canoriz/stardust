(function (root, factory) {
  const api = factory();
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.TraceTimelineModes = api;
})(typeof globalThis !== 'undefined' ? globalThis : this, () => {
  function modeSegments(rows, start, end) {
    if (!rows.length || end <= start) return [];
    const trackAppLimited = rows.some(row => Object.prototype.hasOwnProperty.call(row, 'app_limited'));
    const appLimitedFor = row => trackAppLimited && row.mode === 'ProbeBW' && row.app_limited === true;
    const createSegment = (row, segmentStart, segmentEnd) => {
      const segment = { mode: row.mode, start: segmentStart, end: segmentEnd };
      if (trackAppLimited) segment.app_limited = appLimitedFor(row);
      return segment;
    };
    const segments = [];
    let current = null;
    for (const row of rows) {
      const t = Math.max(start, Math.min(end, row.t));
      const appLimited = appLimitedFor(row);
      if (!current) {
        current = createSegment(row, t, end);
      } else if (row.mode !== current.mode || (trackAppLimited && appLimited !== current.app_limited)) {
        current.end = t;
        segments.push(current);
        current = createSegment(row, t, end);
      }
    }
    if (current && current.end >= current.start) segments.push(current);
    return segments;
  }

  function bandRect(sx, ex, minX, maxX) {
    const rawLeft = Math.floor(Math.min(sx, ex));
    const rawRight = Math.ceil(Math.max(sx, ex));
    const x = Math.max(minX, Math.min(maxX - 1, rawLeft));
    const right = Math.max(x + 1, Math.min(maxX, rawRight));
    return { x, width: right - x };
  }

  function visibleModeSegments(segments, selectedModes) {
    if (!selectedModes.size) return segments;
    return segments.filter(segment => selectedModes.has(
      segment.mode === 'ProbeBW' && segment.app_limited ? 'ProbeBW:app_limited' : segment.mode
    ));
  }

  function toggleModeSelection(selectedModes, key, allModes) {
    if (!selectedModes.size || selectedModes.size === allModes.length) return new Set([key]);
    const next = new Set(selectedModes);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    return next.size === allModes.length ? new Set() : next;
  }

  return { modeSegments, bandRect, visibleModeSegments, toggleModeSelection };
});
