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
        if (current.end > current.start) segments.push(current);
        current = createSegment(row, t, end);
      }
    }
    if (current && current.end > current.start) segments.push(current);
    return segments;
  }

  return { modeSegments };
});
