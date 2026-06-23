// QueryBrew (c) 2025
import { memo, useRef, useEffect } from 'react';
import MonacoEditor from '@monaco-editor/react';
import Box from '@mui/material/Box';
import Stack from '@mui/material/Stack';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import CodeIcon from '@mui/icons-material/Code';
import { useTheme } from '@mui/material/styles';
import { safeFormatSQL } from '../utils/sqlFormat';
import { MONACO_SQL_OPTIONS, addFormatSqlAction } from '../utils/monaco';

interface MonacoProps {
   // Additional imports for the format button
   value: string;
   setValue: (value: string | undefined) => void;
   readOnly?: boolean;
   onCtrlEnter?: () => void;
   onAltLeft?: () => void;
   onAltRight?: () => void;
}

/**
 * Monaco editor component with SQL syntax highlighting
 */
const Monaco = memo(function Monaco({
   value,
   setValue,
   readOnly,
   onCtrlEnter,
   onAltLeft,
   onAltRight,
}: MonacoProps) {
   const theme = useTheme();
   const onCtrlEnterRef = useRef(onCtrlEnter);
   const onAltLeftRef = useRef(onAltLeft);
   const onAltRightRef = useRef(onAltRight);

   // Keep refs updated with latest callbacks
   useEffect(() => {
      onCtrlEnterRef.current = onCtrlEnter;
      onAltLeftRef.current = onAltLeft;
      onAltRightRef.current = onAltRight;
   });

   return (
      <MonacoEditor
         height="100%"
         width="100%"
         defaultLanguage="sql"
         value={value}
         onChange={setValue}
         onMount={(editor, monaco) => {
            // Ctrl+Enter to run all queries
            if (onCtrlEnter) {
               editor.addAction({
                  id: 'run-all-queries',
                  label: 'Run All Queries',
                  keybindings: [
                     monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter,
                     monaco.KeyMod.Alt | monaco.KeyCode.Enter,
                  ],
                  run: () => {
                     if (onCtrlEnterRef.current) {
                        onCtrlEnterRef.current();
                     }
                  },
               });
            }
            // Alt+Left to go to previous query
            if (onAltLeft) {
               editor.addAction({
                  id: 'prev-query',
                  label: 'Previous Query',
                  keybindings: [monaco.KeyMod.Alt | monaco.KeyCode.LeftArrow],
                  run: () => {
                     if (onAltLeftRef.current) {
                        onAltLeftRef.current();
                     }
                  },
               });
            }
            // Alt+Right to go to next query
            if (onAltRight) {
               editor.addAction({
                  id: 'next-query',
                  label: 'Next Query',
                  keybindings: [monaco.KeyMod.Alt | monaco.KeyCode.RightArrow],
                  run: () => {
                     if (onAltRightRef.current) {
                        onAltRightRef.current();
                     }
                  },
               });
            }

            // Ctrl+Shift+F to format SQL
            addFormatSqlAction(editor, monaco, setValue);
         }}
         theme={theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'}
         options={{ ...MONACO_SQL_OPTIONS, readOnly }}
      />
   );
});

interface SQLViewProps {
   title: string;
   children?: React.ReactNode;
   value: string;
   setValue: (value: string | undefined) => void;
   readOnly?: boolean;
   onCtrlEnter?: () => void;
   onAltLeft?: () => void;
   onAltRight?: () => void;
}

/**
 * Component representing a SQL editor with title bar
 */
export default function SQLView({
   title,
   children,
   value,
   setValue,
   readOnly,
   onCtrlEnter,
   onAltLeft,
   onAltRight,
}: SQLViewProps) {
   const handleFormatClick = () => {
      const formatted = safeFormatSQL(value, 'format button');
      if (formatted !== value) {
         setValue(formatted);
      }
   };
   return (
      <Box sx={{ height: '100%', width: '100%', display: 'flex', flexDirection: 'column' }}>
         <Box
            sx={{
               height: '48px',
               minHeight: '48px',
               maxHeight: '48px',
               flexShrink: 0,
               backgroundColor: '#8882',
               borderBottom: '0.5px solid #8888',
            }}
         >
            <Stack
               direction="row"
               spacing={2}
               sx={{ height: '100%', pl: 2, pr: 2, alignItems: 'center' }}
            >
               <strong>{title}</strong>
               <Box sx={{ flexGrow: 1 }} />
               <Tooltip title="Format SQL (Ctrl+Shift+F)">
                  <IconButton size="small" onClick={handleFormatClick}>
                     <CodeIcon sx={{ fontSize: 18 }} />
                  </IconButton>
               </Tooltip>
               {children}
            </Stack>
         </Box>
         <Box sx={{ flex: 1, minHeight: 0 }}>
            <Monaco
               value={value}
               setValue={setValue}
               readOnly={readOnly}
               onCtrlEnter={onCtrlEnter}
               onAltLeft={onAltLeft}
               onAltRight={onAltRight}
            />
         </Box>
      </Box>
   );
}
