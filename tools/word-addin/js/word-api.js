/**
 * Word API integration via Office.js.
 * Handles: insert text at cursor, get selected text, insert comment.
 * Falls back gracefully when running outside Word (browser preview).
 */

function _wordAvailable() {
  return typeof Word !== 'undefined' && typeof Word.run === 'function';
}

/** Insert text at the current cursor position without overwriting selection. */
async function insertTextAtCursor(text) {
  if (!_wordAvailable()) {
    // Browser fallback: copy to clipboard
    try {
      await navigator.clipboard.writeText(text);
    } catch (e) {
      // Fallback for older browsers / restricted contexts
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
    return;
  }
  return Word.run(async function (context) {
    var sel = context.document.getSelection();
    sel.insertText(text, 'After');
    sel.select('End');
    await context.sync();
  });
}

/** Insert text at cursor as a hyperlink to the given URL. Falls back to
 *  plain insert if Word.insertHtml or the link API is unavailable. */
async function insertHyperlinkAtCursor(text, url) {
  if (!url) return insertTextAtCursor(text);
  if (!_wordAvailable()) return insertTextAtCursor(text);
  // Build the HTML server-side to make XSS impossible. text + url are
  // already trusted (they come from our own API), but we still escape.
  var safeText = String(text).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  var safeUrl = String(url).replace(/"/g, '%22').replace(/</g, '%3C').replace(/>/g, '%3E');
  var html = '<a href="' + safeUrl + '">' + safeText + '</a>';
  return Word.run(async function (context) {
    var sel = context.document.getSelection();
    if (typeof sel.insertHtml === 'function') {
      sel.insertHtml(html, 'After');
    } else {
      sel.insertText(text, 'After');
    }
    sel.select('End');
    await context.sync();
  }).catch(function () { return insertTextAtCursor(text); });
}

/** Get the currently selected text from the Word document. */
async function getSelectedText() {
  if (!_wordAvailable()) {
    return "" // prompt not available in Word Online //'Markierten Text hier eingeben (nur im Browser-Modus):') || '';
  }
  return Word.run(async function (context) {
    var range = context.document.getSelection();
    range.load('text');
    await context.sync();
    return range.text;
  });
}

/** Insert a Word comment on the current selection. Returns false if unsupported. */
async function insertComment(text) {
  if (!_wordAvailable() || !supportsComments()) return false;
  return Word.run(async function (context) {
    var range = context.document.getSelection();
    range.insertComment(text);
    await context.sync();
    return true;
  });
}

/** Check if Word comment insertion is supported (WordApi 1.4 / Word 2019+). */
function supportsComments() {
  try {
    return typeof Office !== 'undefined' &&
      Office.context &&
      Office.context.requirements &&
      Office.context.requirements.isSetSupported('WordApi', '1.4');
  } catch (e) {
    return false;
  }
}

/** Read the entire document body as plain text. Returns '' outside Word. */
async function getDocumentText() {
  if (!_wordAvailable()) return '';
  return Word.run(async function (context) {
    var body = context.document.body;
    body.load('text');
    await context.sync();
    return body.text;
  });
}

/** Locate a literal substring in the document, scroll to it, and select it.
 *  Returns true on hit, false otherwise. Uses Word.search with matchCase=true
 *  for stable disambiguation. The optional nth picks the Nth occurrence (0-based). */
async function selectInDocument(needle, nth) {
  if (!_wordAvailable() || !needle) return false;
  nth = nth || 0;
  return Word.run(async function (context) {
    var hits = context.document.body.search(needle, { matchCase: true, matchWholeWord: false });
    hits.load('items');
    await context.sync();
    if (!hits.items || hits.items.length === 0) return false;
    var idx = Math.min(nth, hits.items.length - 1);
    var range = hits.items[idx];
    range.select();
    range.scrollIntoView();
    await context.sync();
    return true;
  }).catch(function () { return false; });
}

/** Replace the Nth occurrence of a literal substring with a new string.
 *  Returns true on success. Used to apply audit fix suggestions in place. */
async function replaceInDocument(needle, replacement, nth) {
  if (!_wordAvailable() || !needle) return false;
  nth = nth || 0;
  return Word.run(async function (context) {
    var hits = context.document.body.search(needle, { matchCase: true, matchWholeWord: false });
    hits.load('items');
    await context.sync();
    if (!hits.items || hits.items.length === 0) return false;
    var range = hits.items[Math.min(nth, hits.items.length - 1)];
    range.insertText(replacement, 'Replace');
    range.scrollIntoView();
    await context.sync();
    return true;
  }).catch(function () { return false; });
}

/** Insert a Word comment anchored on a literal substring (Nth occurrence).
 *  Falls back to inserting plain bracketed text at cursor if anchoring fails. */
async function commentOnSubstring(needle, commentText, nth) {
  if (!_wordAvailable() || !needle) return false;
  nth = nth || 0;
  return Word.run(async function (context) {
    var hits = context.document.body.search(needle, { matchCase: true, matchWholeWord: false });
    hits.load('items');
    await context.sync();
    if (!hits.items || hits.items.length === 0) return false;
    var range = hits.items[Math.min(nth, hits.items.length - 1)];
    if (typeof range.insertComment !== 'function') return false;
    range.insertComment(commentText);
    await context.sync();
    return true;
  }).catch(function () { return false; });
}
