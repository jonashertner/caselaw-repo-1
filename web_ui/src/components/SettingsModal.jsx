import React, { useState, useEffect } from 'react';
import { getKeyStatus, setApiKey, removeApiKey, getOllamaStatus, setOllamaUrl } from '../api';
import { useI18n } from '../i18n';

const PROVIDER_INFO = [
  {
    id: 'claude',
    label: 'Claude (Anthropic)',
    url: 'https://console.anthropic.com/',
    urlLabel: 'console.anthropic.com',
    noteKey: 'settings.claudeNote',
    placeholder: 'sk-ant-...',
  },
  {
    id: 'openai',
    label: 'OpenAI',
    url: 'https://platform.openai.com/api-keys',
    urlLabel: 'platform.openai.com',
    placeholder: 'sk-...',
  },
  {
    id: 'gemini',
    label: 'Gemini (Google)',
    url: 'https://aistudio.google.com/apikey',
    urlLabel: 'aistudio.google.com',
    placeholder: 'AI...',
  },
];

export default function SettingsModal({ open, onClose, onKeysChanged }) {
  const { t } = useI18n();
  const [keyStatus, setKeyStatus] = useState({});
  const [inputs, setInputs] = useState({});
  const [saving, setSaving] = useState({});
  const [error, setError] = useState(null);

  // Ollama state
  const [ollamaReachable, setOllamaReachable] = useState(false);
  const [ollamaUrl, setOllamaUrl_] = useState('http://localhost:11434');
  const [ollamaSaving, setOllamaSaving] = useState(false);

  useEffect(() => {
    if (open) {
      setError(null);
      setInputs({});
      getKeyStatus().then(setKeyStatus).catch(() => {});
      getOllamaStatus()
        .then(data => {
          setOllamaReachable(data.reachable);
          if (data.base_url) setOllamaUrl_(data.base_url);
        })
        .catch(() => {});
    }
  }, [open]);

  if (!open) return null;

  const handleSave = async (providerId) => {
    const key = inputs[providerId]?.trim();
    if (!key) return;
    setSaving(s => ({ ...s, [providerId]: true }));
    setError(null);
    try {
      const result = await setApiKey(providerId, key);
      if (result.ok) {
        setKeyStatus(prev => ({
          ...prev,
          [providerId]: { configured: true, masked: result.masked },
        }));
        setInputs(prev => ({ ...prev, [providerId]: '' }));
        onKeysChanged?.();
      }
    } catch (e) {
      setError(`Failed to save ${providerId} key: ${e.message}`);
    } finally {
      setSaving(s => ({ ...s, [providerId]: false }));
    }
  };

  const handleRemove = async (providerId) => {
    setSaving(s => ({ ...s, [providerId]: true }));
    setError(null);
    try {
      await removeApiKey(providerId);
      setKeyStatus(prev => ({
        ...prev,
        [providerId]: { configured: false, masked: null },
      }));
      onKeysChanged?.();
    } catch (e) {
      setError(`Failed to remove ${providerId} key: ${e.message}`);
    } finally {
      setSaving(s => ({ ...s, [providerId]: false }));
    }
  };

  const handleOllamaSave = async () => {
    const url = ollamaUrl.trim();
    if (!url) return;
    setOllamaSaving(true);
    setError(null);
    try {
      const result = await setOllamaUrl(url);
      if (result.ok) {
        setOllamaReachable(result.reachable);
        onKeysChanged?.();
      }
    } catch (e) {
      setError(`Failed to set Ollama URL: ${e.message}`);
    } finally {
      setOllamaSaving(false);
    }
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-card" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h2>{t('settings.title')}</h2>
          <button className="modal-close" onClick={onClose}>&times;</button>
        </div>

        {error && <div className="modal-error">{error}</div>}

        <div className="modal-body">
          {PROVIDER_INFO.map(p => {
            const status = keyStatus[p.id] || {};
            const isConfigured = status.configured;
            return (
              <div key={p.id} className="provider-row">
                <div className="provider-row-header">
                  <span className={`status-dot ${isConfigured ? 'status-green' : 'status-gray'}`} />
                  <span className="provider-row-name">{p.label}</span>
                  {isConfigured && status.masked && (
                    <code className="provider-row-masked">{status.masked}</code>
                  )}
                  {!isConfigured && (
                    <span className="provider-row-status">{t('settings.notConfigured')}</span>
                  )}
                </div>

                <div className="provider-row-actions">
                  <input
                    type="password"
                    className="key-input"
                    placeholder={isConfigured ? t('settings.replaceKey') : p.placeholder}
                    value={inputs[p.id] || ''}
                    onChange={e => setInputs(prev => ({ ...prev, [p.id]: e.target.value }))}
                    onKeyDown={e => e.key === 'Enter' && handleSave(p.id)}
                    disabled={saving[p.id]}
                  />
                  <button
                    className="btn-save"
                    onClick={() => handleSave(p.id)}
                    disabled={!inputs[p.id]?.trim() || saving[p.id]}
                  >
                    {saving[p.id] ? '...' : t('settings.save')}
                  </button>
                  {isConfigured && (
                    <button
                      className="btn-remove"
                      onClick={() => handleRemove(p.id)}
                      disabled={saving[p.id]}
                    >
                      {t('settings.remove')}
                    </button>
                  )}
                </div>

                <div className="provider-row-help">
                  {t('settings.getKey')}{' '}
                  <a href={p.url} target="_blank" rel="noopener noreferrer">{p.urlLabel}</a>
                </div>
              </div>
            );
          })}

          {/* Ollama / Local Models Section */}
          <div className="ollama-section">
            <div className="ollama-section-header">
              <span className="provider-row-name">{t('settings.ollamaTitle')}</span>
              <span className={`status-dot ${ollamaReachable ? 'status-green' : 'status-gray'}`} />
              <span className={`ollama-status-label ${ollamaReachable ? '' : 'ollama-offline'}`}>
                {ollamaReachable ? t('settings.ollamaConnected') : t('settings.ollamaOffline')}
              </span>
            </div>

            <div className="provider-row-actions">
              <input
                type="text"
                className="key-input"
                placeholder="http://localhost:11434"
                value={ollamaUrl}
                onChange={e => setOllamaUrl_(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleOllamaSave()}
                disabled={ollamaSaving}
              />
              <button
                className="btn-save"
                onClick={handleOllamaSave}
                disabled={!ollamaUrl.trim() || ollamaSaving}
              >
                {ollamaSaving ? '...' : t('settings.save')}
              </button>
            </div>

            <div className="provider-row-help">
              {t('settings.ollamaHelp')}{' '}
              <a href="https://ollama.com" target="_blank" rel="noopener noreferrer">ollama.com</a>
            </div>
            <div className="ollama-models-hint">
              <code>ollama pull qwen2.5:14b</code>
              <code>ollama pull llama3.3:70b</code>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
