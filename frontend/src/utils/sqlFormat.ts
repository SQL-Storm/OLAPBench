// QueryBrew (c) 2025
import { format, type FormatOptionsWithLanguage } from 'sql-formatter';

function ensureTrailingNewline(value: string): string {
   return value.endsWith('\n') ? value : `${value}\n`;
}

const DEFAULT_FORMAT_OPTIONS: FormatOptionsWithLanguage = {
   language: 'postgresql',
   keywordCase: 'upper',
   indentStyle: 'standard',
   linesBetweenQueries: 1,
   denseOperators: true,
   expressionWidth: 200,
};

export function safeFormatSQL(query: string, context: string): string {
   try {
      const formatted = format(query, DEFAULT_FORMAT_OPTIONS);
      return ensureTrailingNewline(formatted);
   } catch (err) {
      console.warn(`SQL formatting failed (${context}); leaving query unformatted.`, err);
      return ensureTrailingNewline(query);
   }
}

// Backwards-compatible alias
export const safeFormatSql = safeFormatSQL;
