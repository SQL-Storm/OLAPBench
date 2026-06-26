// QueryBrew (c) 2025
import { useCallback, useEffect, useMemo, useRef } from 'react';
import MonacoEditor from '@monaco-editor/react';
import Box from '@mui/material/Box';
import Chip from '@mui/material/Chip';
import CircularProgress from '@mui/material/CircularProgress';
import Stack from '@mui/material/Stack';
import ToggleButton from '@mui/material/ToggleButton';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import StorageIcon from '@mui/icons-material/Storage';
import { useTheme } from '@mui/material/styles';
import { PlannerStatisticsResponse } from '../Api';
import ResizablePanels from './ResizablePanels';

interface StatisticsViewProps {
   statistics: PlannerStatisticsResponse | null;
   draft: string;
   draftError?: string | null;
   isLoading?: boolean;
   useStatistics?: boolean;
   onDraftChange: (value: string) => void;
   onToggleUseStatistics?: (enabled: boolean) => void;
}

const editorViewStates = new Map<string, unknown>();

function generationSql(statistics: PlannerStatisticsResponse): string {
   const call = [statistics.statsql_call, statistics.statjson_call].filter(Boolean).join('\n');
   const query = statistics.statsql_query || '';
   return [call, query].filter(Boolean).join('\n\n');
}

export default function StatisticsView({
   statistics,
   draft,
   draftError,
   isLoading,
   useStatistics = false,
   onDraftChange,
   onToggleUseStatistics,
}: StatisticsViewProps) {
   const theme = useTheme();
   const editorRef = useRef<any>(null);
   const editorDisposablesRef = useRef<Array<{ dispose: () => void }>>([]);
   const statisticsModelPath = useMemo(() => {
      const target = statistics?.target_dbms || statistics?.target_dbms_name || 'unknown';
      const dialect = statistics?.dialect || 'stats';
      return `planner-statistics-${target}-${dialect}.json`;
   }, [statistics?.target_dbms, statistics?.target_dbms_name, statistics?.dialect]);

   const saveEditorViewState = useCallback(() => {
      const editor = editorRef.current;
      if (!editor) return;
      editorViewStates.set(statisticsModelPath, editor.saveViewState());
   }, [statisticsModelPath]);

   const handleJsonEditorMount = useCallback(
      (editor: any) => {
         editorRef.current = editor;
         editorDisposablesRef.current.forEach((disposable) => disposable.dispose());
         editorDisposablesRef.current = [];

         const savedViewState = editorViewStates.get(statisticsModelPath);
         if (savedViewState) {
            window.requestAnimationFrame(() => {
               editor.restoreViewState(savedViewState);
            });
         }

         const save = () => saveEditorViewState();
         const hiddenAreasDisposable = editor.onDidChangeHiddenAreas?.(save);
         editorDisposablesRef.current = [
            editor.onDidBlurEditorWidget(save),
            editor.onDidChangeCursorPosition(save),
            editor.onDidScrollChange(save),
            ...(hiddenAreasDisposable ? [hiddenAreasDisposable] : []),
         ];
      },
      [saveEditorViewState, statisticsModelPath]
   );

   const handleDraftChange = useCallback(
      (value?: string) => {
         saveEditorViewState();
         onDraftChange(value || '');
      },
      [onDraftChange, saveEditorViewState]
   );

   useEffect(() => {
      return () => {
         saveEditorViewState();
         editorDisposablesRef.current.forEach((disposable) => disposable.dispose());
         editorDisposablesRef.current = [];
      };
   }, [saveEditorViewState]);

   if (isLoading) {
      return (
         <Box
            sx={{
               height: '100%',
               display: 'flex',
               alignItems: 'center',
               justifyContent: 'center',
            }}
         >
            <CircularProgress size={24} sx={{ mr: 1 }} />
            <Typography variant="body2" color="text.secondary">
               Loading planner statistics...
            </Typography>
         </Box>
      );
   }

   if (!statistics) {
      return (
         <Box
            sx={{
               height: '100%',
               display: 'flex',
               flexDirection: 'column',
               alignItems: 'center',
               justifyContent: 'center',
               gap: 1,
               color: '#888',
            }}
         >
            <Typography variant="body2" color={draftError ? 'error' : 'text.secondary'}>
               {draftError || 'No planner statistics loaded'}
            </Typography>
         </Box>
      );
   }

   const sqlText = generationSql(statistics) || '-- No statistics generation SQL available';

   return (
      <Box sx={{ height: '100%', display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
         <Box
            sx={{
               px: 1,
               py: 0.75,
               borderBottom: '0.5px solid #8888',
               backgroundColor: '#8881',
            }}
         >
            <Stack direction="row" spacing={1} alignItems="center">
               <Chip
                  label={statistics.collection_method || 'statistics'}
                  size="small"
                  color="secondary"
                  variant="outlined"
               />
               {statistics.dialect && (
                  <Chip label={statistics.dialect} size="small" variant="outlined" />
               )}
               {statistics.cache?.hit !== undefined && (
                  <Chip
                     label={statistics.cache.hit ? 'cache hit' : 'collected'}
                     size="small"
                     variant="outlined"
                  />
               )}
               {draftError && (
                  <Typography variant="body2" color="error" noWrap>
                     {draftError}
                  </Typography>
               )}
               <Box sx={{ flexGrow: 1 }} />
               <Tooltip
                  title={
                     useStatistics
                        ? 'Statistics JSON is used for optimization'
                        : 'Statistics JSON is not used for optimization'
                  }
               >
                  <ToggleButton
                     value="use-statistics"
                     selected={useStatistics}
                     onChange={() => onToggleUseStatistics?.(!useStatistics)}
                     size="small"
                     sx={{ height: 28, px: 1, py: 0.5 }}
                  >
                     <StorageIcon sx={{ fontSize: 16, mr: 0.5 }} />
                     <Typography variant="caption">
                        {useStatistics ? 'Used' : 'Ignored'}
                     </Typography>
                  </ToggleButton>
               </Tooltip>
            </Stack>
         </Box>
         <Box sx={{ flex: 1, minHeight: 0 }}>
            <ResizablePanels direction="vertical" initialSize={35} minSize={20}>
               <MonacoEditor
                  height="100%"
                  width="100%"
                  defaultLanguage="sql"
                  value={sqlText}
                  theme={theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'}
                  options={{
                     fontSize: 13,
                     minimap: { enabled: false },
                     lineNumbers: 'on',
                     scrollBeyondLastLine: false,
                     readOnly: true,
                  }}
               />
               <MonacoEditor
                  height="100%"
                  width="100%"
                  defaultLanguage="json"
                  path={statisticsModelPath}
                  saveViewState
                  value={draft}
                  onMount={handleJsonEditorMount}
                  onChange={handleDraftChange}
                  theme={theme.palette.mode === 'dark' ? 'vs-dark' : 'vs'}
                  options={{
                     fontSize: 13,
                     minimap: { enabled: false },
                     lineNumbers: 'on',
                     scrollBeyondLastLine: false,
                     readOnly: false,
                  }}
               />
            </ResizablePanels>
         </Box>
      </Box>
   );
}
