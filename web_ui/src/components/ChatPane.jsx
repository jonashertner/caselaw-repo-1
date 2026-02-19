import React, { useState, useRef, useEffect, useMemo } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

// Statute: "Art. 271 OR", "Art. 8 Abs. 2 BV", etc.
const STATUTE_RE = /\bArt\.?\s*\d+(?:\s*(?:Abs|al|cpv)\.?\s*\d+)?\s*[A-Z][A-Z0-9]{1,11}\b/g;
// Docket: "6B_123/2024", "2C_37/2016", "D-7414/2015"
const DOCKET_RE = /\b[A-Z0-9]{1,4}[._-]\d{1,6}[/_]\d{4}\b/g;
// BGE: "BGE 147 I 268"
const BGE_RE = /\bBGE\s+\d{2,3}\s+[IVX]{1,4}\s+\d{1,4}\b/g;

/**
 * Split text into an array of strings and React elements for statutes/dockets.
 */
function processText(text, onArticleClick, onCitationClick) {
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
      // Statute match
      parts.push(
        <button
          key={`s-${match.index}`}
          className="statute-link"
          onClick={(e) => { e.stopPropagation(); onArticleClick?.(matched); }}
          title={`Search decisions citing ${matched}`}
        >
          {matched}
        </button>
      );
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
function makeLinkifyComponents(onArticleClick, onCitationClick) {
  const wrapChildren = (children) => {
    if (!children) return children;
    if (typeof children === 'string') {
      return processText(children, onArticleClick, onCitationClick);
    }
    if (Array.isArray(children)) {
      return children.map((child, i) =>
        typeof child === 'string'
          ? <React.Fragment key={i}>{processText(child, onArticleClick, onCitationClick)}</React.Fragment>
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

export default function ChatPane({ messages, onSend, isStreaming, onStop, onArticleClick, onCitationClick }) {
  const [input, setInput] = useState('');
  const messagesEndRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSubmit = (e) => {
    e.preventDefault();
    if (input.trim()) {
      onSend(input.trim());
      setInput('');
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }
  };

  const mdComponents = useMemo(
    () => makeLinkifyComponents(onArticleClick, onCitationClick),
    [onArticleClick, onCitationClick]
  );

  return (
    <div className="chat-pane">
      <div className="messages">
        {messages.length === 0 && (
          <div className="empty-state">
            <p>Search Swiss court decisions in natural language.</p>
            <p className="hint">Try: "Find BGer decisions on rental termination from 2024"</p>
            <p className="hint">Or: "Suche Urteile zum Mietrecht mit Bezug auf Art. 271 OR"</p>
            <p className="hint">Or: "Trouvez des arrets du TF sur le droit du bail"</p>
          </div>
        )}
        {messages.map((msg) => (
          <div key={msg.id} className={`message message-${msg.role}`}>
            <div className="message-role">
              {msg.role === 'user' ? 'You' : msg.role === 'assistant' ? 'Assistant' : 'Tool'}
            </div>
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
            <span className="dot-pulse" />
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      <form className="chat-input" onSubmit={handleSubmit}>
        <textarea
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="Ask about Swiss case law..."
          rows={2}
          disabled={isStreaming}
        />
        {isStreaming ? (
          <button type="button" onClick={onStop} className="btn-stop">Stop</button>
        ) : (
          <button type="submit" disabled={!input.trim()} className="btn-send">Send</button>
        )}
      </form>
    </div>
  );
}
