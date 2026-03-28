/**
 * Word API integration via Office.js.
 * Handles: insert text at cursor, get selected text, insert comment.
 */

/** Insert text at the current cursor position without overwriting selection. */
async function insertTextAtCursor(text) {
  return Word.run(async function (context) {
    var range = context.document.getSelection();
    range.insertText(text, Word.InsertLocation.after);
    range.select(Word.SelectionMode.end);
    await context.sync();
  });
}

/** Get the currently selected text from the Word document. */
async function getSelectedText() {
  return Word.run(async function (context) {
    var range = context.document.getSelection();
    range.load('text');
    await context.sync();
    return range.text;
  });
}

/** Insert a Word comment on the current selection. Returns false if unsupported. */
async function insertComment(text) {
  if (!supportsComments()) return false;
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
    return Office.context.requirements.isSetSupported('WordApi', '1.4');
  } catch (e) {
    return false;
  }
}
