import React, { useState, useCallback, useEffect, useRef } from 'react';
import { getDecision } from '../api';

/** Strip all HTML tags except <mark>, </mark>, and <br>. */
function sanitizeSnippet(html) {
  if (!html) return '';
  return html.replace(/<\/?(?!mark\b|br\b)[a-z][^>]*>/gi, '');
}

export default function ResultsPane({ decisions, highlightId, onHighlightClear }) {
  const [expandedId, setExpandedId] = useState(null);
  const [fullTexts, setFullTexts] = useState({});
  const cardRefs = useRef({});

  const handleExpand = useCallback((lookupId) => {
    if (expandedId === lookupId) {
      setExpandedId(null);
      return;
    }
    setExpandedId(lookupId);

    if (!lookupId || fullTexts[lookupId]) return;

    setFullTexts(prev => ({ ...prev, [lookupId]: { loading: true } }));
    getDecision(lookupId)
      .then(data => {
        setFullTexts(prev => ({
          ...prev,
          [lookupId]: { loading: false, content: data.content || 'No content available.' },
        }));
      })
      .catch(err => {
        setFullTexts(prev => ({
          ...prev,
          [lookupId]: { loading: false, error: err.message },
        }));
      });
  }, [expandedId, fullTexts]);

  // Auto-expand and scroll to highlighted decision
  useEffect(() => {
    if (!highlightId) return;

    const match = decisions.find(d => d.docket_number === highlightId);
    if (!match) return;

    const lookupId = match.decision_id || match.docket_number;
    setExpandedId(lookupId);

    // Scroll after a tick so DOM updates
    requestAnimationFrame(() => {
      const el = cardRefs.current[lookupId];
      if (el) el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
    });

    // Clear highlight after the animation
    const timer = setTimeout(() => onHighlightClear?.(), 1500);
    return () => clearTimeout(timer);
  }, [highlightId, decisions, onHighlightClear]);

  if (decisions.length === 0) {
    return (
      <div className="results-pane">
        <h3>Decisions</h3>
        <p className="empty-hint">Search results will appear here.</p>
      </div>
    );
  }

  return (
    <div className="results-pane">
      <h3>Decisions ({decisions.length})</h3>
      <div className="decision-list">
        {decisions.map((d, i) => {
          const lookupId = d.decision_id || d.docket_number;
          const cardKey = lookupId || `_idx_${i}`;
          const isExpanded = expandedId === lookupId && lookupId != null;
          const isHighlighted = highlightId && d.docket_number === highlightId;
          const ft = lookupId ? fullTexts[lookupId] : null;

          return (
            <div
              key={cardKey}
              ref={el => { if (lookupId) cardRefs.current[lookupId] = el; }}
              className={
                `decision-card${isExpanded ? ' expanded' : ''}${isHighlighted ? ' highlighted' : ''}`
              }
              onClick={() => handleExpand(lookupId)}
            >
              <div className="decision-header">
                <span className="decision-docket">{d.docket_number || 'Unknown'}</span>
                <span className="decision-date">{d.decision_date || ''}</span>
              </div>
              <div className="decision-meta">
                {d.court && <span className="tag">{d.court}</span>}
                {d.language && <span className="tag">{d.language.toUpperCase()}</span>}
              </div>
              {d.title && <div className="decision-title">{d.title}</div>}
              {isExpanded && (
                <div className="decision-details">
                  {d.regeste && (
                    <div
                      className="decision-regeste"
                      dangerouslySetInnerHTML={{ __html: sanitizeSnippet(d.regeste) }}
                    />
                  )}
                  {d.snippet && (
                    <div
                      className="decision-snippet"
                      dangerouslySetInnerHTML={{ __html: sanitizeSnippet(d.snippet) }}
                    />
                  )}
                  {d.source_url && (
                    <a href={d.source_url} target="_blank" rel="noopener" className="decision-link"
                       onClick={e => e.stopPropagation()}>
                      View source
                    </a>
                  )}
                  {ft && ft.loading && (
                    <div className="decision-fulltext-loading">
                      <span className="dot-pulse" /> Loading full text...
                    </div>
                  )}
                  {ft && ft.error && (
                    <div className="decision-fulltext-error">Failed to load: {ft.error}</div>
                  )}
                  {ft && ft.content && (
                    <div className="decision-fulltext">
                      <div className="decision-fulltext-label">Full Text</div>
                      <div className="decision-fulltext-prose">{ft.content}</div>
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
