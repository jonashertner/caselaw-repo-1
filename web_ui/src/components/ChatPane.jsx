import React, { useState, useRef, useEffect, useMemo, useCallback } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { useI18n } from '../i18n';
import { fetchStatuteText } from '../api';

// Statute: "Art. 271 OR", "Art. 271a OR", "Art. 261bis StGB", "Art. 8 Abs. 2 BV", etc.
// Law code: requires 2+ uppercase letters (catches StGB, SchKG, ArG, VwVG, BankG, etc.)
const STATUTE_RE = /\b[Aa]rt\.?\s*\d+(?:\s*[a-z]{1,6})?(?:\s*(?:Abs|al|cpv)\.?\s*\d+)?(?:\s*(?:ff|let|lit|Bst)\.?\s*[a-z]?)?\s*[A-Z][A-Za-z]{0,4}[A-Z][A-Za-z0-9]{0,7}\b/g;
// Docket: "6B_123/2024", "2C_37/2016", "D-7414/2015"
const DOCKET_RE = /\b[A-Z0-9]{1,4}[._-]\d{1,6}[/_]\d{4}\b/g;
// BGE: "BGE 147 I 268"
const BGE_RE = /\bBGE\s+\d{2,3}\s+[IVX]{1,4}\s+\d{1,4}\b/g;

/**
 * Fedlex Classified Compilation (eli/cc/) paths for common Swiss federal laws.
 * Maps law abbreviation (DE/FR/IT) → AS publication path.
 */
const FEDLEX_CC = {
  // Constitution
  BV: '1999/404', Cst: '1999/404', Cost: '1999/404',

  // Core private law
  OR: '27/317_321_377', CO: '27/317_321_377',
  ZGB: '24/233_245_233', CC: '24/233_245_233',

  // Criminal law
  StGB: '54/757_781_799', CP: '54/757_781_799',

  // Procedural law
  ZPO: '2010/262', CPC: '2010/262',
  StPO: '2010/267', CPP: '2010/267',
  BGG: '2006/218', LTF: '2006/218',
  VwVG: '1969/737_755_755', PA: '1969/737_755_755',
  VGG: '2006/2197', LTAF: '2006/2197',

  // Debt enforcement & bankruptcy
  SchKG: '11/529_545_529', LP: '11/529_545_529',

  // Social insurance
  AHVG: '1947/669_669_669', LAVS: '1947/669_669_669',
  IVG: '1959/827_857_845', LAI: '1959/827_857_845',
  BVG: '1983/797_797_797', LPP: '1983/797_797_797',
  UVG: '1982/1676_1676_1676', LAA: '1982/1676_1676_1676',
  AVIG: '1982/2184_2184_2184', LACI: '1982/2184_2184_2184',

  // Tax
  DBG: '1991/1184_1184_1184', LIFD: '1991/1184_1184_1184',
  StHG: '1991/1256_1256_1256', LHID: '1991/1256_1256_1256',
  MWSTG: '2009/5203', LTVA: '2009/5203',

  // Transport
  SVG: '1959/679_705_685', LCR: '1959/679_705_685',

  // Labor
  ArG: '1966/57_65_57', LTr: '1966/57_65_57',

  // Intellectual property
  URG: '1993/1798_1798_1798', LDA: '1993/1798_1798_1798',
  MSchG: '1993/274_274_274', LPM: '1993/274_274_274',
  PatG: '1955/893_909_893', LBI: '1955/893_909_893',

  // Environment & planning
  USG: '1984/1122_1122_1122', LPE: '1984/1122_1122_1122',
  RPG: '1979/1573_1573_1573', LAT: '1979/1573_1573_1573',
  NHG: '1966/1637_1659_1621', LPN: '1966/1637_1659_1621',

  // Competition
  KG: '1996/546_546_546', LCart: '1996/546_546_546',

  // Financial markets
  BankG: '1935/170_176_170', LB: '1935/170_176_170',
  FINMAG: '2008/5207', LFINMA: '2008/5207',

  // Data protection (new, 2023)
  DSG: '2022/491', LPD: '2022/491',
};

const STATUTE_PARSE_RE = /[Aa]rt\.?\s*(\d+)\s*[a-z]{0,6}\s*(?:(Abs|al|cpv)\.?\s*\d+)?(?:\s*(?:ff|let|lit|Bst)\.?\s*[a-z]?)?\s*([A-Z][A-Za-z]{0,4}[A-Z][A-Za-z0-9]{0,7})/;

/**
 * Build a Fedlex URL for a statute reference like "Art. 271 OR".
 * Returns the URL string, or null if the law is not in our mapping.
 */
function buildFedlexUrl(text) {
  const m = text.match(STATUTE_PARSE_RE);
  if (!m) return null;

  const artNum = m[1];
  const parType = m[2]; // Abs, al, cpv — indicates language
  const law = m[3];

  const ccPath = FEDLEX_CC[law];
  if (!ccPath) return null;

  // Detect language from paragraph abbreviation
  let lang = 'de';
  if (parType === 'al') lang = 'fr';
  else if (parType === 'cpv') lang = 'it';

  return `https://www.fedlex.admin.ch/eli/cc/${ccPath}/${lang}#art_${artNum}`;
}

/**
 * Split text into an array of strings and React elements for statutes/dockets.
 */
function processText(text, onCitationClick, onStatuteClick) {
  if (typeof text !== 'string' || !text) return text;

  // Build a combined pattern
  const combined = new RegExp(
    `(${STATUTE_RE.source})|(${BGE_RE.source})|(${DOCKET_RE.source})`,
    'g'
  );

  const parts = [];
  let lastIndex = 0;

  for (const match of text.matchAll(combined)) {
    if (match.index > lastIndex) {
      parts.push(text.slice(lastIndex, match.index));
    }

    const matched = match[0];
    if (match[1]) {
      // Statute match — open popup with Fedlex article text
      const url = buildFedlexUrl(matched);
      if (url) {
        const artMatch = matched.match(/Art\.?\s*(\d+)/);
        const artNum = artMatch ? artMatch[1] : '';
        parts.push(
          <button
            key={`s-${match.index}`}
            className="statute-link"
            onClick={(e) => { e.stopPropagation(); onStatuteClick?.({ text: matched, url, article: artNum }); }}
            title={matched}
          >
            {matched}
          </button>
        );
      } else {
        // Unknown law — render as plain text
        parts.push(matched);
      }
    } else if (match[2]) {
      // BGE match
      parts.push(
        <button
          key={`b-${match.index}`}
          className="citation-link"
          onClick={(e) => { e.stopPropagation(); onCitationClick?.(matched); }}
          title={`Show ${matched}`}
        >
          {matched}
        </button>
      );
    } else if (match[3]) {
      // Docket match
      parts.push(
        <button
          key={`d-${match.index}`}
          className="citation-link"
          onClick={(e) => { e.stopPropagation(); onCitationClick?.(matched); }}
          title={`Show ${matched}`}
        >
          {matched}
        </button>
      );
    }

    lastIndex = match.index + matched.length;
  }

  if (lastIndex < text.length) {
    parts.push(text.slice(lastIndex));
  }

  return parts.length > 0 ? parts : text;
}

/**
 * Create custom ReactMarkdown components that linkify citations in text nodes.
 */
function makeLinkifyComponents(onCitationClick, onStatuteClick) {
  const wrapChildren = (children) => {
    if (!children) return children;
    if (typeof children === 'string') {
      return processText(children, onCitationClick, onStatuteClick);
    }
    if (Array.isArray(children)) {
      return children.map((child, i) =>
        typeof child === 'string'
          ? <React.Fragment key={i}>{processText(child, onCitationClick, onStatuteClick)}</React.Fragment>
          : child
      );
    }
    return children;
  };

  // Intercept text-containing elements
  const make = (Tag) => ({ children, ...props }) => (
    <Tag {...props}>{wrapChildren(children)}</Tag>
  );

  return {
    p: make('p'),
    li: make('li'),
    td: make('td'),
    strong: make('strong'),
    em: make('em'),
  };
}

const AssistantAvatar = () => (
  <div className="message-avatar message-avatar-assistant">
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polygon points="12 2 2 7 12 12 22 7 12 2"/>
      <polyline points="2 17 12 22 22 17"/>
      <polyline points="2 12 12 17 22 12"/>
    </svg>
  </div>
);

export default function ChatPane({ messages, onSend, isStreaming, onStop, onCitationClick }) {
  const { t } = useI18n();
  const [input, setInput] = useState('');
  const [openStatute, setOpenStatute] = useState(null);
  const [statuteContent, setStatuteContent] = useState({ loading: false, html: null, text: null, error: null });
  const messagesEndRef = useRef(null);
  const textareaRef = useRef(null);

  const handleStatuteClick = useCallback((statute) => {
    setOpenStatute(statute);
    setStatuteContent({ loading: true, html: null, text: null, error: null });
    fetchStatuteText(statute.url, statute.article)
      .then(data => {
        setStatuteContent({ loading: false, html: data.html, text: data.text, error: null });
      })
      .catch(() => {
        setStatuteContent({ loading: false, html: null, text: null, error: 'Failed to load' });
      });
  }, []);

  const suggestions = useMemo(() => [t('suggestion.0'), t('suggestion.1'), t('suggestion.2')], [t]);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSubmit = (e) => {
    e.preventDefault();
    if (input.trim()) {
      onSend(input.trim());
      setInput('');
      if (textareaRef.current) {
        textareaRef.current.style.height = 'auto';
      }
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  const handleInputChange = (e) => {
    setInput(e.target.value);
    const el = e.target;
    el.style.height = 'auto';
    el.style.height = Math.min(el.scrollHeight, 120) + 'px';
  };

  const mdComponents = useMemo(
    () => makeLinkifyComponents(onCitationClick, handleStatuteClick),
    [onCitationClick, handleStatuteClick]
  );

  return (
    <div className="chat-pane">
      <div className="messages">
        {messages.length === 0 && (
          <div className="empty-state">
            <div className="empty-state-icon">{'\u2696'}</div>
            <div className="empty-state-title">{t('empty.title')}</div>
            <div className="empty-state-desc">{t('empty.desc')}</div>
            <div className="suggestion-chips">
              {suggestions.map((s, i) => (
                <button
                  key={i}
                  className="suggestion-chip"
                  onClick={() => onSend(s)}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}
        {messages.map((msg) => (
          <div key={msg.id} className={`message message-${msg.role}`}>
            {msg.role === 'user' && (
              <div className="message-avatar message-avatar-user">Y</div>
            )}
            {msg.role === 'assistant' && <AssistantAvatar />}
            <div className="message-content">
              {msg.role === 'tool' && msg.type === 'start' ? (
                <span className="tool-calling">{msg.content}</span>
              ) : (
                <ReactMarkdown
                  remarkPlugins={[remarkGfm]}
                  components={msg.role === 'assistant' ? mdComponents : undefined}
                >
                  {msg.content || ''}
                </ReactMarkdown>
              )}
            </div>
          </div>
        ))}
        {isStreaming && (
          <div className="streaming-indicator">
            <div className="typing-dots">
              <span className="typing-dot" />
              <span className="typing-dot" />
              <span className="typing-dot" />
            </div>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      <div className="chat-input-wrap">
        <form className="chat-input-bar" onSubmit={handleSubmit}>
          <textarea
            ref={textareaRef}
            value={input}
            onChange={handleInputChange}
            onKeyDown={handleKeyDown}
            placeholder={t('input.placeholder')}
            rows={1}
            disabled={isStreaming}
          />
          {isStreaming ? (
            <button type="button" onClick={onStop} className="btn-stop-circle" title={t('btn.stop')}>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor">
                <rect x="6" y="6" width="12" height="12" rx="2"/>
              </svg>
            </button>
          ) : (
            <button type="submit" disabled={!input.trim()} className="btn-send-circle" title={t('btn.send')}>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <line x1="12" y1="19" x2="12" y2="5"/>
                <polyline points="5 12 12 5 19 12"/>
              </svg>
            </button>
          )}
        </form>
      </div>

      {openStatute && (
        <div className="statute-popup-overlay" onClick={() => setOpenStatute(null)}>
          <div className="statute-popup" onClick={e => e.stopPropagation()}>
            <div className="statute-popup-header">
              <h3>{openStatute.text}</h3>
              <div className="statute-popup-actions">
                <a href={openStatute.url} target="_blank" rel="noopener noreferrer" className="statute-popup-fedlex">
                  Fedlex ↗
                </a>
                <button className="modal-close" onClick={() => setOpenStatute(null)}>&times;</button>
              </div>
            </div>
            <div className="statute-popup-body">
              {statuteContent.loading && (
                <div className="statute-popup-loading">
                  <div className="typing-dots">
                    <span className="typing-dot" />
                    <span className="typing-dot" />
                    <span className="typing-dot" />
                  </div>
                </div>
              )}
              {statuteContent.html && (
                <div
                  className="statute-popup-text"
                  dangerouslySetInnerHTML={{ __html: statuteContent.html }}
                />
              )}
              {!statuteContent.html && statuteContent.text && (
                <div className="statute-popup-text">{statuteContent.text}</div>
              )}
              {!statuteContent.loading && !statuteContent.html && !statuteContent.text && (
                <div className="statute-popup-fallback">
                  {statuteContent.error
                    ? <p>{statuteContent.error}</p>
                    : <p>Article text not available.</p>
                  }
                  <a href={openStatute.url} target="_blank" rel="noopener noreferrer">
                    View on Fedlex ↗
                  </a>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
