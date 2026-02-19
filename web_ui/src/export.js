/**
 * Export conversation in Markdown, DOCX, or PDF.
 * The docx library is lazy-loaded to avoid adding ~350KB to the initial bundle.
 */

const datestamp = () => new Date().toISOString().split('T')[0];

function download(blob, filename) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

/** Collect chat messages into a simple {role, content}[] array. */
function collectMessages(messages, t) {
  const out = [];
  for (const msg of messages) {
    if (msg.role === 'user') {
      out.push({ role: t('role.user'), content: msg.content || '' });
    } else if (msg.role === 'assistant') {
      out.push({ role: t('role.assistant'), content: msg.content || '' });
    }
  }
  return out;
}

// ── Markdown ──────────────────────────────────────────────

export function exportMarkdown(messages, t) {
  const items = collectMessages(messages, t);
  if (!items.length) return;

  const lines = [`# ${t('export.title')}\n`, `${datestamp()}\n`];
  for (const m of items) {
    lines.push(`\n## ${m.role}\n`);
    lines.push(m.content);
    lines.push('');
  }
  const blob = new Blob([lines.join('\n')], { type: 'text/markdown;charset=utf-8' });
  download(blob, `caselaw-export-${datestamp()}.md`);
}

// ── DOCX (lazy-loaded) ───────────────────────────────────

export async function exportDocx(messages, t) {
  const items = collectMessages(messages, t);
  if (!items.length) return;

  const { Document, Packer, Paragraph, TextRun, HeadingLevel } = await import('docx');

  /** Parse a markdown line into TextRun objects with basic formatting. */
  function mdToRuns(line) {
    const runs = [];
    const re = /(\*\*(.+?)\*\*|\*(.+?)\*|`(.+?)`|([^*`]+))/g;
    let m;
    while ((m = re.exec(line)) !== null) {
      if (m[2]) runs.push(new TextRun({ text: m[2], bold: true }));
      else if (m[3]) runs.push(new TextRun({ text: m[3], italics: true }));
      else if (m[4]) runs.push(new TextRun({ text: m[4], font: 'Courier New', size: 20 }));
      else if (m[5]) runs.push(new TextRun({ text: m[5] }));
    }
    return runs.length ? runs : [new TextRun({ text: line })];
  }

  /** Convert markdown content string into docx Paragraph objects. */
  function mdToParagraphs(content) {
    const paragraphs = [];
    const headingLevels = [HeadingLevel.HEADING_1, HeadingLevel.HEADING_2, HeadingLevel.HEADING_3, HeadingLevel.HEADING_4];
    for (const line of content.split('\n')) {
      const hm = line.match(/^(#{1,4})\s+(.+)/);
      if (hm) {
        paragraphs.push(new Paragraph({
          heading: headingLevels[hm[1].length - 1] || HeadingLevel.HEADING_4,
          children: [new TextRun({ text: hm[2] })],
        }));
        continue;
      }
      if (line.match(/^\s*[-*]\s+/)) {
        paragraphs.push(new Paragraph({
          bullet: { level: 0 },
          children: mdToRuns(line.replace(/^\s*[-*]\s+/, '')),
          spacing: { after: 60 },
        }));
        continue;
      }
      const nm = line.match(/^\s*\d+\.\s+(.+)/);
      if (nm) {
        paragraphs.push(new Paragraph({
          bullet: { level: 0 },
          children: mdToRuns(nm[1]),
          spacing: { after: 60 },
        }));
        continue;
      }
      if (!line.trim()) continue;
      paragraphs.push(new Paragraph({
        children: mdToRuns(line),
        spacing: { after: 120 },
      }));
    }
    return paragraphs;
  }

  const children = [
    new Paragraph({
      heading: HeadingLevel.TITLE,
      children: [new TextRun({ text: t('export.title') })],
    }),
    new Paragraph({
      children: [new TextRun({ text: datestamp(), color: '666666', size: 20 })],
      spacing: { after: 300 },
    }),
  ];

  for (const m of items) {
    children.push(new Paragraph({
      heading: HeadingLevel.HEADING_2,
      children: [new TextRun({ text: m.role })],
      spacing: { before: 300 },
    }));
    children.push(...mdToParagraphs(m.content));
  }

  const doc = new Document({
    sections: [{ children }],
    styles: {
      default: {
        document: {
          run: { font: 'Calibri', size: 22 },
        },
      },
    },
  });

  const buffer = await Packer.toBlob(doc);
  download(buffer, `caselaw-export-${datestamp()}.docx`);
}

// ── PDF (via print) ───────────────────────────────────────

function esc(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

function inlineFormat(s) {
  s = esc(s);
  s = s.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  s = s.replace(/\*(.+?)\*/g, '<em>$1</em>');
  s = s.replace(/`(.+?)`/g, '<code>$1</code>');
  return s;
}

/** Minimal markdown → HTML. */
function markdownToHtml(md) {
  const lines = md.split('\n');
  const out = [];
  let inList = false;
  let listTag = 'ul';

  for (const line of lines) {
    const hm = line.match(/^(#{1,4})\s+(.+)/);
    if (hm) {
      if (inList) { out.push(`</${listTag}>`); inList = false; }
      out.push(`<h${hm[1].length + 1}>${inlineFormat(hm[2])}</h${hm[1].length + 1}>`);
      continue;
    }
    if (line.match(/^\s*[-*]\s+/)) {
      if (!inList) { listTag = 'ul'; out.push('<ul>'); inList = true; }
      out.push(`<li>${inlineFormat(line.replace(/^\s*[-*]\s+/, ''))}</li>`);
      continue;
    }
    const nm = line.match(/^\s*\d+\.\s+(.+)/);
    if (nm) {
      if (!inList) { listTag = 'ol'; out.push('<ol>'); inList = true; }
      out.push(`<li>${inlineFormat(nm[1])}</li>`);
      continue;
    }
    if (inList) { out.push(`</${listTag}>`); inList = false; }
    if (!line.trim()) continue;
    out.push(`<p>${inlineFormat(line)}</p>`);
  }
  if (inList) out.push(`</${listTag}>`);
  return out.join('\n');
}

export function exportPdf(messages, t) {
  const items = collectMessages(messages, t);
  if (!items.length) return;

  let body = `<h1>${esc(t('export.title'))}</h1>\n<p class="date">${datestamp()}</p>\n`;
  for (const m of items) {
    body += `<h2>${esc(m.role)}</h2>\n${markdownToHtml(m.content)}\n`;
  }

  const html = `<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<title>${esc(t('export.title'))}</title>
<style>
  @page { margin: 2cm; size: A4; }
  body { font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; font-size: 11pt; line-height: 1.55; color: #1a1a1a; max-width: 700px; margin: 0 auto; padding: 20px; }
  h1 { font-size: 20pt; margin-bottom: 4px; }
  h2 { font-size: 13pt; color: #444; border-bottom: 1px solid #ddd; padding-bottom: 4px; margin-top: 28px; }
  h3 { font-size: 11pt; margin-top: 16px; }
  h4 { font-size: 10.5pt; margin-top: 12px; }
  p { margin: 6px 0; }
  .date { color: #888; font-size: 10pt; margin-bottom: 16px; }
  ul, ol { padding-left: 22px; margin: 6px 0; }
  li { margin: 3px 0; }
  code { font-family: 'SFMono-Regular', Consolas, 'Courier New', monospace; font-size: 10pt; background: #f3f3f3; padding: 1px 4px; border-radius: 3px; }
  strong { font-weight: 600; }
  @media print {
    body { padding: 0; }
  }
</style>
</head><body>${body}</body></html>`;

  const w = window.open('', '_blank');
  if (!w) return;
  w.document.write(html);
  w.document.close();
  w.addEventListener('afterprint', () => w.close());
  setTimeout(() => w.print(), 300);
}
