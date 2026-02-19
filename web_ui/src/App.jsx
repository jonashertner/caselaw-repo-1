import React, { useState, useRef, useCallback, useEffect } from 'react';
import { streamChat, checkHealth, searchDecisions } from './api';
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

export default function App() {
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
  const abortRef = useRef(null);

  // Apply theme to document
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    localStorage.setItem('theme', theme);
  }, [theme]);

  // Fetch provider status on mount
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

  // Auto-select first configured provider when status loads/changes
  useEffect(() => {
    if (!providerStatus) return;
    const current = providerStatus[provider];
    if (current?.configured && current?.sdk_installed) return; // current is fine
    const ready = Object.entries(providerStatus).find(
      ([, s]) => s.configured && s.sdk_installed
    );
    if (ready) setProvider(ready[0]);
  }, [providerStatus]); // eslint-disable-line react-hooks/exhaustive-deps

  const toggleTheme = () => setTheme(t => t === 'light' ? 'dark' : 'light');

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

    // Only send filters when user has set something beyond defaults
    const defaults = { collapse_duplicates: true, multilingual: true };
    const cleanFilters = {};
    for (const [k, v] of Object.entries(filters)) {
      if (v !== '' && v !== null) cleanFilters[k] = v;
    }
    const hasUserFilters = Object.entries(cleanFilters).some(
      ([k, v]) => !(k in defaults && defaults[k] === v)
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
              setMessages(prev => {
                // Remove the "Calling..." message
                return prev.filter(m => !(m.role === 'tool' && m.type === 'start'));
              });
              break;

            case 'decisions':
              if (chunk.decisions) {
                // Deduplicate by docket_number, keeping latest
                setDecisions(prev => {
                  const merged = [...prev, ...chunk.decisions];
                  const seen = new Map();
                  // Iterate in reverse so later entries win
                  for (let i = merged.length - 1; i >= 0; i--) {
                    const key = merged[i].docket_number || `_idx_${i}`;
                    if (!seen.has(key)) seen.set(key, merged[i]);
                  }
                  // Reverse to restore original order
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
        const key = merged[i].docket_number || `_idx_${i}`;
        if (!seen.has(key)) seen.set(key, merged[i]);
      }
      return [...seen.values()].reverse();
    });
  }, []);

  const handleSearchArticle = useCallback(async (articleRef) => {
    try {
      const data = await searchDecisions({ query: articleRef });
      if (data.decisions?.length) mergeDecisions(data.decisions);
    } catch {
      // ignore — article search is best-effort
    }
  }, [mergeDecisions]);

  const handleCitationClick = useCallback(async (docket) => {
    // If already in results, just highlight
    const found = decisions.find(d => d.docket_number === docket);
    if (found) {
      setHighlightId(docket);
      return;
    }
    // Otherwise fetch and highlight
    try {
      const data = await searchDecisions({ query: `"${docket}"` });
      if (data.decisions?.length) {
        mergeDecisions(data.decisions);
        // Use the docket from the first result or the requested docket
        setHighlightId(data.decisions[0]?.docket_number || docket);
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

  return (
    <div className="app">
      <header className="app-header">
        <h1>Swiss Case Law Search</h1>
        <div className="header-controls">
          <ProviderSelector
            value={provider}
            onChange={setProvider}
            disabled={isStreaming}
            providerStatus={providerStatus}
          />
          <button onClick={() => setShowFilters(f => !f)} className="btn-secondary">
            Filters {showFilters ? '\u25B2' : '\u25BC'}
          </button>
          <button onClick={newSession} className="btn-secondary">New Session</button>
          <button
            onClick={() => setShowSettings(true)}
            className="btn-secondary btn-settings"
            title="API Key Settings"
          >
            Settings
          </button>
          <button onClick={toggleTheme} className="btn-secondary btn-theme" title="Toggle dark mode">
            {theme === 'light' ? '\u263E' : '\u2600'}
          </button>
          {(tokenUsage.input > 0 || tokenUsage.output > 0) && (
            <span className="token-counter" title="Token usage this session">
              {tokenUsage.input.toLocaleString()} in / {tokenUsage.output.toLocaleString()} out
            </span>
          )}
        </div>
      </header>

      {noProvidersConfigured && (
        <div className="setup-banner">
          No API keys configured.{' '}
          <button className="setup-banner-link" onClick={() => setShowSettings(true)}>
            Open Settings
          </button>{' '}
          to add one.
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
            onArticleClick={handleSearchArticle}
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
            <div className="tool-traces">
              <h3>Tool Activity</h3>
              {toolTraces.map((t, i) => (
                <div key={i} className="trace-item">
                  <span className="trace-tool">{t.tool}</span>
                  <span className="trace-latency">{t.latency_ms}ms</span>
                  {t.hit_count != null && (
                    <span className="trace-hits">{t.hit_count} results</span>
                  )}
                </div>
              ))}
            </div>
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
