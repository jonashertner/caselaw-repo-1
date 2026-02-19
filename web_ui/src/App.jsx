import React, { useState, useRef, useCallback, useEffect, useMemo } from 'react';
import { streamChat, checkHealth, searchDecisions } from './api';
import { useI18n, LANGS } from './i18n';
import { exportMarkdown, exportDocx, exportPdf } from './export';
import ProviderSelector from './components/ProviderSelector';
import ChatPane from './components/ChatPane';
import ResultsPane from './components/ResultsPane';
import Filters from './components/Filters';
import SettingsModal from './components/SettingsModal';

let _msgIdCounter = 0;
function nextMsgId() { return `msg_${++_msgIdCounter}_${Date.now()}`; }

function getInitialTheme() {
  const saved = localStorage.getItem('theme');
  if (saved === 'dark' || saved === 'light') return saved;
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

const FILTER_DEFAULTS = { collapse_duplicates: true, multilingual: true };

export default function App() {
  const { lang, setLang, t } = useI18n();
  const [provider, setProvider] = useState('claude');
  const [messages, setMessages] = useState([]);
  const [decisions, setDecisions] = useState([]);
  const [toolTraces, setToolTraces] = useState([]);
  const [isStreaming, setIsStreaming] = useState(false);
  const [sessionId, setSessionId] = useState(null);
  const [filters, setFilters] = useState({
    court: '', canton: '', language: '', date_from: '', date_to: '',
    collapse_duplicates: true, multilingual: true,
  });
  const [showFilters, setShowFilters] = useState(false);
  const [error, setError] = useState(null);
  const [theme, setTheme] = useState(getInitialTheme);
  const [providerStatus, setProviderStatus] = useState(null);
  const [showSettings, setShowSettings] = useState(false);
  const [highlightId, setHighlightId] = useState(null);
  const [tokenUsage, setTokenUsage] = useState({ input: 0, output: 0 });
  const [showExportMenu, setShowExportMenu] = useState(false);
  const exportMenuRef = useRef(null);
  const abortRef = useRef(null);

  const filtersActive = useMemo(() => {
    return Object.entries(filters).some(([k, v]) => {
      if (k in FILTER_DEFAULTS) return v !== FILTER_DEFAULTS[k];
      return v !== '';
    });
  }, [filters]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('theme', theme);
  }, [theme]);

  const refreshProviderStatus = useCallback(() => {
    checkHealth()
      .then(data => {
        if (data.providers && typeof data.providers === 'object' && !Array.isArray(data.providers)) {
          setProviderStatus(data.providers);
        }
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    refreshProviderStatus();
  }, [refreshProviderStatus]);

  useEffect(() => {
    if (!providerStatus) return;
    const current = providerStatus[provider];
    if (current?.configured && current?.sdk_installed) return;
    const ready = Object.entries(providerStatus).find(
      ([, s]) => s.configured && s.sdk_installed
    );
    if (ready) setProvider(ready[0]);
  }, [providerStatus]); // eslint-disable-line react-hooks/exhaustive-deps

  const toggleTheme = () => setTheme(t2 => t2 === 'light' ? 'dark' : 'light');

  const noProvidersConfigured = providerStatus &&
    Object.values(providerStatus).every(s => !s.configured);

  const sendMessage = useCallback(async (text) => {
    if (!text.trim() || isStreaming) return;

    setError(null);
    setMessages(prev => [...prev, { id: nextMsgId(), role: 'user', content: text }]);
    setIsStreaming(true);

    const abortController = new AbortController();
    abortRef.current = abortController;

    let assistantContent = '';
    const assistantId = nextMsgId();

    const cleanFilters = {};
    for (const [k, v] of Object.entries(filters)) {
      if (v !== '' && v !== null) cleanFilters[k] = v;
    }
    const hasUserFilters = Object.entries(cleanFilters).some(
      ([k, v]) => !(k in FILTER_DEFAULTS && FILTER_DEFAULTS[k] === v)
    );

    try {
      await streamChat(
        {
          provider,
          message: text,
          session_id: sessionId,
          filters: hasUserFilters ? cleanFilters : undefined,
        },
        (chunk) => {
          switch (chunk.type) {
            case 'text':
              assistantContent += chunk.content;
              setMessages(prev => {
                const last = prev[prev.length - 1];
                if (last?.id === assistantId) {
                  return [...prev.slice(0, -1), { ...last, content: assistantContent }];
                }
                return [...prev, { id: assistantId, role: 'assistant', content: assistantContent }];
              });
              break;

            case 'tool_start':
              setMessages(prev => [
                ...prev,
                { id: nextMsgId(), role: 'tool', content: chunk.content, type: 'start' },
              ]);
              break;

            case 'tool_end':
              setToolTraces(prev => [...prev, chunk.tool_trace]);
              setMessages(prev => prev.filter(m => !(m.role === 'tool' && m.type === 'start')));
              break;

            case 'decisions':
              if (chunk.decisions) {
                setDecisions(prev => {
                  const merged = [...prev, ...chunk.decisions];
                  const seen = new Map();
                  for (let i = merged.length - 1; i >= 0; i--) {
                    const key = merged[i].decision_id || merged[i].docket_number || `_idx_${i}`;
                    if (!seen.has(key)) seen.set(key, merged[i]);
                  }
                  return [...seen.values()].reverse();
                });
              }
              break;

            case 'done':
              if (chunk.session_id) setSessionId(chunk.session_id);
              if (chunk.input_tokens || chunk.output_tokens) {
                setTokenUsage(prev => ({
                  input: prev.input + (chunk.input_tokens || 0),
                  output: prev.output + (chunk.output_tokens || 0),
                }));
              }
              break;

            case 'error':
              setError(chunk.content);
              break;
          }
        },
        abortController.signal,
      );
    } catch (e) {
      if (e.name !== 'AbortError') {
        setError(e.message);
      }
    } finally {
      setIsStreaming(false);
      abortRef.current = null;
    }
  }, [provider, sessionId, filters, isStreaming]);

  const stopStreaming = () => {
    if (abortRef.current) abortRef.current.abort();
  };

  const mergeDecisions = useCallback((newDecs) => {
    setDecisions(prev => {
      const merged = [...prev, ...newDecs];
      const seen = new Map();
      for (let i = merged.length - 1; i >= 0; i--) {
        const key = merged[i].decision_id || merged[i].docket_number || `_idx_${i}`;
        if (!seen.has(key)) seen.set(key, merged[i]);
      }
      return [...seen.values()].reverse();
    });
  }, []);

  const handleCitationClick = useCallback(async (docket) => {
    const found = decisions.find(d =>
      d.docket_number === docket || d.decision_id === docket
    );
    if (found) {
      setHighlightId(found.decision_id || found.docket_number);
      return;
    }
    try {
      const data = await searchDecisions({ query: `"${docket}"` });
      if (data.decisions?.length) {
        mergeDecisions(data.decisions);
        setHighlightId(data.decisions[0]?.decision_id || data.decisions[0]?.docket_number || docket);
      }
    } catch (e) {
      // ignore
    }
  }, [decisions, mergeDecisions]);

  const newSession = () => {
    setSessionId(null);
    setMessages([]);
    setDecisions([]);
    setToolTraces([]);
    setError(null);
    setHighlightId(null);
    setTokenUsage({ input: 0, output: 0 });
  };

  const handleExport = useCallback((format) => {
    setShowExportMenu(false);
    if (messages.length === 0) return;
    if (format === 'md') exportMarkdown(messages, t);
    else if (format === 'docx') exportDocx(messages, t);
    else if (format === 'pdf') exportPdf(messages, t);
  }, [messages, t]);

  // Close export menu on outside click
  useEffect(() => {
    if (!showExportMenu) return;
    const handler = (e) => {
      if (exportMenuRef.current && !exportMenuRef.current.contains(e.target)) {
        setShowExportMenu(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [showExportMenu]);

  const totalTraceMs = useMemo(
    () => toolTraces.reduce((sum, t2) => sum + (t2.latency_ms || 0), 0),
    [toolTraces]
  );

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-brand">
          <div className="header-brand-title">
            <span className="header-brand-icon">{'\u2696'}</span>
            <span>Caselaw</span>
          </div>
          <div className="header-brand-subtitle">{t('header.subtitle')}</div>
        </div>

        <div className="header-center">
          <ProviderSelector
            value={provider}
            onChange={setProvider}
            disabled={isStreaming}
            providerStatus={providerStatus}
          />
        </div>

        <div className="header-actions">
          <button className="icon-btn" onClick={() => setShowFilters(f => !f)} title={t('btn.filters')}>
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
              <path d="M2 4h12M4 8h8M6 12h4"/>
            </svg>
            {filtersActive && <span className="icon-btn-badge" />}
          </button>
          <button className="icon-btn" onClick={newSession} title={t('btn.new')}>
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round">
              <path d="M8 3v10M3 8h10"/>
            </svg>
          </button>
          <div className="export-dropdown" ref={exportMenuRef}>
            <button
              className="icon-btn"
              onClick={() => setShowExportMenu(v => !v)}
              title={t('btn.export')}
              disabled={messages.length === 0}
            >
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
                <polyline points="7 10 12 15 17 10"/>
                <line x1="12" y1="15" x2="12" y2="3"/>
              </svg>
            </button>
            {showExportMenu && (
              <div className="export-menu">
                <button onClick={() => handleExport('md')}>Markdown (.md)</button>
                <button onClick={() => handleExport('docx')}>Word (.docx)</button>
                <button onClick={() => handleExport('pdf')}>PDF</button>
              </div>
            )}
          </div>
          <div className="header-divider" />
          <button className="icon-btn" onClick={() => setShowSettings(true)} title={t('btn.settings')}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="3"/>
              <path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 01-2.83 2.83l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 009 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06A1.65 1.65 0 004.68 15a1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 9a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06A1.65 1.65 0 009 4.68a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06A1.65 1.65 0 0019.4 9a1.65 1.65 0 001.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/>
            </svg>
          </button>
          <button className="icon-btn" onClick={toggleTheme} title={t('btn.theme')}>
            {theme === 'light' ? (
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z"/>
              </svg>
            ) : (
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="5"/>
                <line x1="12" y1="1" x2="12" y2="3"/><line x1="12" y1="21" x2="12" y2="23"/>
                <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>
                <line x1="1" y1="12" x2="3" y2="12"/><line x1="21" y1="12" x2="23" y2="12"/>
                <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>
              </svg>
            )}
          </button>
          <div className="header-divider" />
          <div className="lang-switcher">
            {LANGS.map(l => (
              <button
                key={l}
                className={`lang-btn${lang === l ? ' active' : ''}`}
                onClick={() => setLang(l)}
              >
                {l.toUpperCase()}
              </button>
            ))}
          </div>
          {(tokenUsage.input > 0 || tokenUsage.output > 0) && (
            <span className="token-counter" title="Token usage">
              {tokenUsage.input.toLocaleString()} in / {tokenUsage.output.toLocaleString()} out
            </span>
          )}
        </div>
      </header>

      {noProvidersConfigured && (
        <div className="setup-banner">
          {t('banner.noKeys')}{' '}
          <button className="setup-banner-link" onClick={() => setShowSettings(true)}>
            {t('banner.openSettings')}
          </button>{' '}
          {t('banner.toAdd')}
        </div>
      )}

      {showFilters && <Filters filters={filters} onChange={setFilters} />}

      {error && <div className="error-banner">{error}</div>}

      <div className="main-layout">
        <div className="chat-column">
          <ChatPane
            messages={messages}
            onSend={sendMessage}
            isStreaming={isStreaming}
            onStop={stopStreaming}
            onCitationClick={handleCitationClick}
          />
        </div>
        <div className="side-column">
          <ResultsPane
            decisions={decisions}
            highlightId={highlightId}
            onHighlightClear={() => setHighlightId(null)}
          />
          {toolTraces.length > 0 && (
            <details className="tool-traces">
              <summary>
                {toolTraces.length} tool call{toolTraces.length !== 1 ? 's' : ''} — {totalTraceMs}ms total
              </summary>
              <div className="tool-traces-list">
                {toolTraces.map((t2, i) => (
                  <div key={i} className="trace-item">
                    <span className="trace-tool">{t2.tool}</span>
                    <span className="trace-latency">{t2.latency_ms}ms</span>
                    {t2.hit_count != null && (
                      <span className="trace-hits">{t2.hit_count} results</span>
                    )}
                  </div>
                ))}
              </div>
            </details>
          )}
        </div>
      </div>

      <SettingsModal
        open={showSettings}
        onClose={() => setShowSettings(false)}
        onKeysChanged={refreshProviderStatus}
      />
    </div>
  );
}
