import React, { useState, useRef, useEffect } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

export default function ChatPane({ messages, onSend, isStreaming, onStop }) {
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
                <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.content || ''}</ReactMarkdown>
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
