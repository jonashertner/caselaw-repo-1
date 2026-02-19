import React from 'react';

const CLOUD = [
  { id: 'claude', label: 'Claude' },
  { id: 'openai', label: 'OpenAI' },
  { id: 'gemini', label: 'Gemini' },
];
const LOCAL = [
  { id: 'qwen2.5', label: 'Qwen 2.5' },
  { id: 'llama3.3', label: 'Llama 3.3' },
];

function ProviderButton({ p, value, onChange, disabled, providerStatus }) {
  const status = providerStatus?.[p.id];
  const notConfigured = status && !status.configured;
  const noSdk = status && !status.sdk_installed;
  return (
    <button
      key={p.id}
      className={`provider-btn ${value === p.id ? 'active' : ''} ${notConfigured || noSdk ? 'provider-unavailable' : ''}`}
      onClick={() => onChange(p.id)}
      disabled={disabled || notConfigured || noSdk}
      title={noSdk ? `${p.label}: SDK not installed` : notConfigured ? `${p.label}: not configured` : p.label}
    >
      {p.label}
    </button>
  );
}

export default function ProviderSelector({ value, onChange, disabled, providerStatus }) {
  return (
    <div className="provider-selector">
      {CLOUD.map(p => (
        <ProviderButton key={p.id} p={p} value={value} onChange={onChange} disabled={disabled} providerStatus={providerStatus} />
      ))}
      <span className="provider-divider">
        <span className="provider-divider-label">local</span>
      </span>
      {LOCAL.map(p => (
        <ProviderButton key={p.id} p={p} value={value} onChange={onChange} disabled={disabled} providerStatus={providerStatus} />
      ))}
    </div>
  );
}
