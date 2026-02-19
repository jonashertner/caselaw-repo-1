import React, { useState, useCallback } from 'react';
import { getDecision } from '../api';

export default function ResultsPane({ decisions }) {
  const [expandedId, setExpandedId] = useState(null);
  const [fullTexts, setFullTexts] = useState({});  // cache: id → { loading, content, error }

  const handleExpand = useCallback((lookupId) => {
    if (expandedId === lookupId) {
      setExpandedId(null);
      return;
    }
    setExpandedId(lookupId);

    if (!lookupId || fullTexts[lookupId]) return;

    // Fetch full text
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
          const ft = lookupId ? fullTexts[lookupId] : null;

          return (
            <div
              key={cardKey}
              className={`decision-card ${isExpanded ? 'expanded' : ''}`}
              onClick={() => handleExpand(lookupId)}
            >
              <div className="decision-header">
                <span className="decision-docket">{d.docket_number || 'Unknown'}</span>
                <span className="decision-date">{d.decision_date || ''}</span>
              </div>
              <div className="decision-meta">
                {d.court && <span className="tag tag-court">{d.court}</span>}
                {d.language && <span className="tag tag-lang">{d.language}</span>}
              </div>
              {d.title && <div className="decision-title">{d.title}</div>}
              {isExpanded && (
                <div className="decision-details">
                  {d.regeste && <div className="decision-regeste">{d.regeste}</div>}
                  {d.snippet && <div className="decision-snippet">{d.snippet}</div>}
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
