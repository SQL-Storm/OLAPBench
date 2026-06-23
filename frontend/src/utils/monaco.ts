// QueryBrew (c) 2025
import type { ComponentProps } from 'react';
import type { Monaco, OnMount } from '@monaco-editor/react';
import type MonacoEditor from '@monaco-editor/react';
import { safeFormatSQL } from './sqlFormat';

type CodeEditor = Parameters<OnMount>[0];
type EditorOptions = ComponentProps<typeof MonacoEditor>['options'];

/** Shared Monaco editor options for the SQL editors. */
export const MONACO_SQL_OPTIONS: EditorOptions = {
   fontSize: 14,
   minimap: { enabled: false },
   lineNumbers: 'on',
   scrollBeyondLastLine: false,
   scrollbar: {
      vertical: 'hidden',
      horizontal: 'hidden',
      verticalHasArrows: false,
      verticalScrollbarSize: 0,
      verticalSliderSize: 0,
   },
};

/**
 * Register the Ctrl+Shift+F "Format SQL" action on a Monaco editor. Formats the current
 * selection if there is one, otherwise the whole document, and writes the result back
 * through `setValue`.
 */
export function addFormatSqlAction(
   editor: CodeEditor,
   monaco: Monaco,
   setValue: (value: string) => void
): void {
   editor.addAction({
      id: 'format-sql',
      label: 'Format SQL',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyF],
      run: () => {
         const selection = editor.getSelection();
         const model = editor.getModel();
         if (!model) return;

         if (selection && !selection.isEmpty()) {
            const selectedText = model.getValueInRange(selection);
            const formatted = safeFormatSQL(selectedText, 'monaco selection');
            if (formatted !== selectedText) {
               model.pushEditOperations([], [{ range: selection, text: formatted }], () => null);
               setValue(model.getValue());
            }
         } else {
            const full = model.getValue();
            const formatted = safeFormatSQL(full, 'monaco document');
            if (formatted !== full) {
               model.pushEditOperations(
                  [],
                  [{ range: model.getFullModelRange(), text: formatted }],
                  () => null
               );
               setValue(formatted);
            }
         }
      },
   });
}
