import React, { useState, useRef, useCallback, useEffect } from 'react';
import { streamChat, checkHealth } from './api';
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

  const newSession = () => {
    setSessionId(null);
    setMessages([]);
    setDecisions([]);
    setToolTraces([]);
    setError(null);
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
          />
        </div>
        <div className="side-column">
          <ResultsPane decisions={decisions} />
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
