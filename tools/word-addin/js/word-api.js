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
    console.log('Text kopiert: ' + text);
    return;
  }
  return Word.run(async function (context) {
    var range = context.document.getSelection();
    range.insertText(text, Word.InsertLocation.after);
    range.select(Word.SelectionMode.end);
    await context.sync();
  });
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
