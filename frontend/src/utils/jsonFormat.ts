interface JsonTextFormatOptions {
   indent?: number;
}

const DEFAULT_OPTIONS = {
   indent: 2,
};

function indentation(depth: number, size: number): string {
   return ' '.repeat(depth * size);
}

function formatLeafTokens(items: string[]): string {
   return `[${items.join(', ')}]`;
}

function formatLeafObjectEntries(entries: Array<{ key: string; value: string }>): string {
   return `{${entries.map(({ key, value }) => `${key}: ${value}`).join(', ')}}`;
}

class JsonTextFormatter {
   private position = 0;

   constructor(
      private readonly input: string,
      private readonly options: Required<JsonTextFormatOptions>
   ) {}

   format(): string {
      this.skipWhitespace();
      const formatted = this.formatValue(0);
      this.skipWhitespace();
      if (this.position !== this.input.length) {
         throw new Error('Unexpected trailing JSON text');
      }
      return formatted;
   }

   private formatValue(depth: number): string {
      this.skipWhitespace();
      const char = this.peek();
      if (char === '{') {
         return this.formatObject(depth);
      }
      if (char === '[') {
         return this.formatArray(depth);
      }
      if (char === '"') {
         return this.readString();
      }
      return this.readPrimitiveToken();
   }

   private formatObject(depth: number): string {
      const leafObject = this.previewLeafObject(this.position);
      if (leafObject) {
         this.position = leafObject.end;
         return formatLeafObjectEntries(leafObject.entries);
      }

      this.expect('{');
      this.skipWhitespace();

      if (this.peek() === '}') {
         this.position += 1;
         return '{}';
      }

      const entries: string[] = [];
      let closed = false;
      while (this.position < this.input.length) {
         this.skipWhitespace();
         const key = this.readString();
         this.skipWhitespace();
         this.expect(':');
         const value = this.formatValue(depth + 1);
         entries.push(`${indentation(depth + 1, this.options.indent)}${key}: ${value}`);
         this.skipWhitespace();

         const separator = this.peek();
         if (separator === '}') {
            this.position += 1;
            closed = true;
            break;
         }
         this.expect(',');
      }

      if (!closed) {
         throw new Error('Unterminated JSON object');
      }

      return `{\n${entries.join(',\n')}\n${indentation(depth, this.options.indent)}}`;
   }

   private formatArray(depth: number): string {
      const leafArray = this.previewLeafArray(this.position);
      if (leafArray) {
         this.position = leafArray.end;
         return formatLeafTokens(leafArray.items);
      }

      this.expect('[');
      this.skipWhitespace();

      if (this.peek() === ']') {
         this.position += 1;
         return '[]';
      }

      const items: string[] = [];
      let closed = false;
      while (this.position < this.input.length) {
         items.push(
            `${indentation(depth + 1, this.options.indent)}${this.formatValue(depth + 1)}`
         );
         this.skipWhitespace();

         const separator = this.peek();
         if (separator === ']') {
            this.position += 1;
            closed = true;
            break;
         }
         this.expect(',');
      }

      if (!closed) {
         throw new Error('Unterminated JSON array');
      }

      return `[\n${items.join(',\n')}\n${indentation(depth, this.options.indent)}]`;
   }

   private previewLeafArray(start: number): { items: string[]; end: number } | null {
      return this.readLeafArrayAt(start);
   }

   private previewLeafObject(
      start: number
   ): { entries: Array<{ key: string; value: string }>; end: number } | null {
      let cursor = this.skipWhitespaceAt(start);
      if (this.input[cursor] !== '{') {
         return null;
      }
      cursor += 1;
      cursor = this.skipWhitespaceAt(cursor);

      const entries: Array<{ key: string; value: string }> = [];
      if (this.input[cursor] === '}') {
         return { entries, end: cursor + 1 };
      }

      while (cursor < this.input.length) {
         const keyEnd = this.readStringEndAt(cursor);
         if (keyEnd === null) {
            return null;
         }
         const key = this.input.slice(cursor, keyEnd);
         cursor = this.skipWhitespaceAt(keyEnd);

         if (this.input[cursor] !== ':') {
            return null;
         }
         cursor += 1;

         const token = this.readLeafTokenAt(cursor);
         if (!token) {
            return null;
         }
         entries.push({ key, value: token.raw });
         cursor = this.skipWhitespaceAt(token.end);

         if (this.input[cursor] === '}') {
            return { entries, end: cursor + 1 };
         }
         if (this.input[cursor] !== ',') {
            return null;
         }
         cursor += 1;
         cursor = this.skipWhitespaceAt(cursor);
      }

      return null;
   }

   private readLeafArrayAt(start: number): { items: string[]; end: number } | null {
      let cursor = this.skipWhitespaceAt(start);
      if (this.input[cursor] !== '[') {
         return null;
      }
      cursor += 1;
      cursor = this.skipWhitespaceAt(cursor);

      const items: string[] = [];
      if (this.input[cursor] === ']') {
         return { items, end: cursor + 1 };
      }

      while (cursor < this.input.length) {
         const token = this.readLeafTokenAt(cursor);
         if (!token) {
            return null;
         }
         items.push(token.raw);
         cursor = this.skipWhitespaceAt(token.end);

         if (this.input[cursor] === ']') {
            return { items, end: cursor + 1 };
         }
         if (this.input[cursor] !== ',') {
            return null;
         }
         cursor += 1;
         cursor = this.skipWhitespaceAt(cursor);
      }

      return null;
   }

   private readLeafTokenAt(start: number): { raw: string; end: number } | null {
      const cursor = this.skipWhitespaceAt(start);
      if (this.input[cursor] === '[') {
         return this.readEmptyContainerAt(cursor, '[', ']');
      }
      if (this.input[cursor] === '{') {
         return this.readEmptyContainerAt(cursor, '{', '}');
      }
      return this.readPrimitiveTokenAt(cursor);
   }

   private readEmptyContainerAt(
      start: number,
      open: '[' | '{',
      close: ']' | '}'
   ): { raw: string; end: number } | null {
      let cursor = this.skipWhitespaceAt(start);
      if (this.input[cursor] !== open) {
         return null;
      }
      cursor += 1;
      cursor = this.skipWhitespaceAt(cursor);
      if (this.input[cursor] !== close) {
         return null;
      }
      return { raw: `${open}${close}`, end: cursor + 1 };
   }

   private readPrimitiveTokenAt(start: number): { raw: string; end: number } | null {
      let cursor = this.skipWhitespaceAt(start);
      const char = this.input[cursor];
      if (char === undefined || char === '[' || char === '{' || char === ']' || char === '}') {
         return null;
      }

      if (char === '"') {
         const end = this.readStringEndAt(cursor);
         return end === null ? null : { raw: this.input.slice(cursor, end), end };
      }

      const tokenStart = cursor;
      while (cursor < this.input.length) {
         const current = this.input[cursor];
         if (current === ',' || current === ']' || current === '}' || /\s/.test(current)) {
            break;
         }
         cursor += 1;
      }

      return cursor === tokenStart ? null : { raw: this.input.slice(tokenStart, cursor), end: cursor };
   }

   private readString(): string {
      const end = this.readStringEndAt(this.position);
      if (end === null) {
         throw new Error('Unterminated string');
      }
      const raw = this.input.slice(this.position, end);
      this.position = end;
      return raw;
   }

   private readStringEndAt(start: number): number | null {
      let cursor = start;
      if (this.input[cursor] !== '"') {
         return null;
      }
      cursor += 1;

      while (cursor < this.input.length) {
         const char = this.input[cursor];
         if (char === '"') {
            return cursor + 1;
         }
         if (char === '\\') {
            cursor += 2;
            continue;
         }
         cursor += 1;
      }

      return null;
   }

   private readPrimitiveToken(): string {
      const start = this.position;
      while (this.position < this.input.length) {
         const char = this.input[this.position];
         if (char === ',' || char === ']' || char === '}' || /\s/.test(char)) {
            break;
         }
         this.position += 1;
      }
      if (this.position === start) {
         throw new Error('Expected JSON token');
      }
      return this.input.slice(start, this.position);
   }

   private skipWhitespace() {
      this.position = this.skipWhitespaceAt(this.position);
   }

   private skipWhitespaceAt(start: number): number {
      let cursor = start;
      while (cursor < this.input.length && /\s/.test(this.input[cursor])) {
         cursor += 1;
      }
      return cursor;
   }

   private expect(expected: string) {
      if (this.peek() !== expected) {
         throw new Error(`Expected "${expected}"`);
      }
      this.position += 1;
   }

   private peek(): string | undefined {
      return this.input[this.position];
   }
}

export function formatJsonTextPreservingContent(
   value: string,
   options: JsonTextFormatOptions = {}
): string {
   const resolvedOptions: Required<JsonTextFormatOptions> = { ...DEFAULT_OPTIONS, ...options };
   try {
      return new JsonTextFormatter(value, resolvedOptions).format();
   } catch {
      return value;
   }
}

export function stripJsonWhitespaceOutsideStrings(value: string): string {
   let result = '';
   let inString = false;
   let escaped = false;

   for (const char of value) {
      if (inString) {
         result += char;
         if (escaped) {
            escaped = false;
         } else if (char === '\\') {
            escaped = true;
         } else if (char === '"') {
            inString = false;
         }
         continue;
      }

      if (char === '"') {
         inString = true;
         result += char;
      } else if (!/\s/.test(char)) {
         result += char;
      }
   }

   return result;
}
