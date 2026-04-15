import React, { useState, useCallback, useEffect, useRef } from 'react';
import { getDecision } from '../api';
import { useI18n } from '../i18n';

/** Strip all HTML tags except <mark>, </mark>, and <br>. */
function sanitizeSnippet(html) {
  if (!html) return '';
  return html.replace(/<\/?(?!mark\b|br\b)[a-z][^>]*>/gi, '');
}

/** Build a single-line legal citation header for copy-paste. */
function buildCitation(d) {
  const parts = [];
  if (d.docket_number) parts.push(d.docket_number);
  if (d.decision_date) parts.push(`(${d.decision_date})`);
  if (d.court) parts.push(`[${d.court}]`);
  let line = parts.join(' ');
  if (d.title) line += ` — ${d.title}`;
  if (d.source_url) line += `\n${d.source_url}`;
  return line;
}

export default function ResultsPane({ decisions, highlightId, onHighlightClear }) {
  const { t } = useI18n();
  const [expandedId, setExpandedId] = useState(null);
  const [fullTexts, setFullTexts] = useState({});
  const [copiedId, setCopiedId] = useState(null);
  const cardRefs = useRef({});
  const copyTimerRef = useRef(null);

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

  const handleCopyFulltext = useCallback(async (lookupId, decision) => {
    const ft = fullTexts[lookupId];
    if (!ft || !ft.content) return;
    const text = `${buildCitation(decision)}\n\n${ft.content}`;
    try {
      await navigator.clipboard.writeText(text);
      setCopiedId(lookupId);
      if (copyTimerRef.current) clearTimeout(copyTimerRef.current);
      copyTimerRef.current = setTimeout(() => setCopiedId(null), 2000);
    } catch (err) {
      // Clipboard API blocked (non-HTTPS / permission denied) — fall back
      // to selecting the fulltext node so the user can Cmd+C manually.
      const el = cardRefs.current[lookupId]?.querySelector('.decision-fulltext-prose');
      if (el) {
        const range = document.createRange();
        range.selectNodeContents(el);
        const sel = window.getSelection();
        sel?.removeAllRanges();
        sel?.addRange(range);
      }
    }
  }, [fullTexts]);

  // Auto-expand and scroll to highlighted decision
  useEffect(() => {
    if (!highlightId) return;

    const match = decisions.find(d =>
      d.decision_id === highlightId || d.docket_number === highlightId
    );
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

  // Escape collapses the expanded card (standard UX affordance).
  useEffect(() => {
    if (!expandedId) return;
    const onKey = (e) => {
      if (e.key === 'Escape') setExpandedId(null);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [expandedId]);

  // Clean up copy-confirmation timer on unmount.
  useEffect(() => () => {
    if (copyTimerRef.current) clearTimeout(copyTimerRef.current);
  }, []);

  const handleCardKeyDown = useCallback((e, lookupId) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      handleExpand(lookupId);
    }
  }, [handleExpand]);

  if (decisions.length === 0) {
    return (
      <div className="results-pane">
        <h3>{t('results.heading')}</h3>
        <p className="empty-hint">{t('results.empty')}</p>
      </div>
    );
  }

  return (
    <div className="results-pane">
      <h3>{t('results.heading')} ({decisions.length})</h3>
      <div className="decision-list">
        {decisions.map((d, i) => {
          const lookupId = d.decision_id || d.docket_number;
          const cardKey = lookupId || `_idx_${i}`;
          const isExpanded = expandedId === lookupId && lookupId != null;
          const isHighlighted = highlightId && d.docket_number === highlightId;
          const ft = lookupId ? fullTexts[lookupId] : null;
          const isCopied = copiedId === lookupId;

          return (
            <div
              key={cardKey}
              ref={el => { if (lookupId) cardRefs.current[lookupId] = el; }}
              className={
                `decision-card${isExpanded ? ' expanded' : ''}${isHighlighted ? ' highlighted' : ''}`
              }
              onClick={() => handleExpand(lookupId)}
              onKeyDown={(e) => handleCardKeyDown(e, lookupId)}
              role="button"
              tabIndex={0}
              aria-expanded={isExpanded}
              aria-label={d.docket_number
                ? `${d.docket_number}${d.decision_date ? ', ' + d.decision_date : ''}`
                : undefined}
            >
              <div className="decision-header">
                <span className="decision-docket">{d.docket_number || 'Unknown'}</span>
                <span className="decision-date">{d.decision_date || ''}</span>
              </div>
              <div className="decision-meta">
                {d.court && <span className="tag tag-court">{d.court}</span>}
                {d.language && <span className="tag">{d.language.toUpperCase()}</span>}
              </div>
              {d.title && <div className="decision-title">{d.title}</div>}
              {!isExpanded && (d.snippet || d.regeste) && (
                <div
                  className="decision-preview"
                  dangerouslySetInnerHTML={{ __html: sanitizeSnippet(d.snippet || d.regeste) }}
                />
              )}
              {isExpanded && (
                <div
                  className="decision-details"
                  onClick={e => e.stopPropagation()}
                  onMouseDown={e => e.stopPropagation()}
                  onKeyDown={e => e.stopPropagation()}
                >
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
                  <div className="decision-actions">
                    {d.source_url && (
                      <a href={d.source_url} target="_blank" rel="noopener noreferrer" className="decision-link">
                        {t('results.source')}
                      </a>
                    )}
                    {ft && ft.content && (
                      <button
                        type="button"
                        className={`decision-copy-btn${isCopied ? ' copied' : ''}`}
                        onClick={() => handleCopyFulltext(lookupId, d)}
                        aria-label={t('results.copy')}
                      >
                        {isCopied ? t('results.copied') : t('results.copy')}
                      </button>
                    )}
                  </div>
                  {ft && ft.loading && (
                    <div className="decision-fulltext-loading">
                      <span className="dot-pulse" /> {t('results.loading')}
                    </div>
                  )}
                  {ft && ft.error && (
                    <div className="decision-fulltext-error">{t('results.loadError')}: {ft.error}</div>
                  )}
                  {ft && ft.content && (
                    <div className="decision-fulltext">
                      <div className="decision-fulltext-label">{t('results.fulltext')}</div>
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
