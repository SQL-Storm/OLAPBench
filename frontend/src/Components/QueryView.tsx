// QueryBrew (c) 2025
import { useCallback, useEffect, useRef } from 'react';
import Button from '@mui/material/Button';
import PlayArrow from '@mui/icons-material/PlayArrow';
import SQLView from './SQLView';
import QuerySelection from './QuerySelector';
import { Query } from '../Api';

interface QueryViewProps {
   query: string;
   setQuery: React.Dispatch<React.SetStateAction<string>>;
   queries: Query[];
   benchmarkName: string;
   onRunAll: (queryOverride?: string) => void;
   onAutoRunAll: (queryOverride?: string) => void;
   isLoading: boolean;
   currentQueryIndex: number | null;
   setCurrentQueryIndex: React.Dispatch<React.SetStateAction<number | null>>;
   autoRunEnabled: boolean;
}

/**
 * Query input component where users can write or select SQL queries
 */
export default function QueryView({
   query,
   setQuery,
   queries,
   benchmarkName,
   onRunAll,
   onAutoRunAll,
   isLoading,
   currentQueryIndex,
   setCurrentQueryIndex,
   autoRunEnabled,
}: QueryViewProps) {
   const autoRunTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

   useEffect(() => {
      return () => {
         if (autoRunTimerRef.current) {
            clearTimeout(autoRunTimerRef.current);
            autoRunTimerRef.current = null;
         }
      };
   }, []);

   const handleSetValue = (value: string | undefined) => {
      if (value !== undefined) {
         setQuery(value);
         if (autoRunEnabled) {
            if (autoRunTimerRef.current) {
               clearTimeout(autoRunTimerRef.current);
            }
            autoRunTimerRef.current = setTimeout(() => {
               onAutoRunAll(value);
            }, 350);
         }
      }
   };

   // Navigate to previous query and run
   const handlePrevQuery = useCallback(() => {
      if (queries.length === 0) return;

      let newIndex: number;
      if (currentQueryIndex === null || currentQueryIndex <= 0) {
         newIndex = queries.length - 1; // Wrap to last
      } else {
         newIndex = currentQueryIndex - 1;
      }

      setCurrentQueryIndex(newIndex);
      const newSql = queries[newIndex].sql;
      setQuery(newSql);
      if (autoRunEnabled) {
         onAutoRunAll(newSql);
      }
   }, [queries, currentQueryIndex, setCurrentQueryIndex, setQuery, onAutoRunAll, autoRunEnabled]);

   // Navigate to next query and run
   const handleNextQuery = useCallback(() => {
      if (queries.length === 0) return;

      let newIndex: number;
      if (currentQueryIndex === null || currentQueryIndex >= queries.length - 1) {
         newIndex = 0; // Wrap to first
      } else {
         newIndex = currentQueryIndex + 1;
      }

      setCurrentQueryIndex(newIndex);
      const newSql = queries[newIndex].sql;
      setQuery(newSql);
      if (autoRunEnabled) {
         onAutoRunAll(newSql);
      }
   }, [queries, currentQueryIndex, setCurrentQueryIndex, setQuery, onAutoRunAll, autoRunEnabled]);

   // Handle query selection from dropdown - also triggers run
   const handleSelectQuery = useCallback(
      (sql: string) => {
         const index = queries.findIndex((q) => q.sql === sql);
         if (index !== -1) {
            setCurrentQueryIndex(index);
         }
         if (autoRunEnabled) {
            onAutoRunAll(sql);
         }
      },
      [queries, setCurrentQueryIndex, onAutoRunAll, autoRunEnabled]
   );

   // Get current query name for the dropdown
   const currentQueryName =
      currentQueryIndex !== null && queries[currentQueryIndex]
         ? queries[currentQueryIndex].name
         : null;

   return (
      <SQLView
         title="Query"
         value={query}
         setValue={handleSetValue}
         onCtrlEnter={onRunAll}
         onAltLeft={handlePrevQuery}
         onAltRight={handleNextQuery}
      >
         {queries.length > 0 && (
            <QuerySelection
               queries={queries}
               name={benchmarkName}
               setSqlQuery={setQuery}
               onSelectQuery={handleSelectQuery}
               selectedQueryName={currentQueryName}
            />
         )}
         <Button
            variant="contained"
            color="primary"
            onClick={() => onRunAll()}
            disabled={isLoading || !query.trim()}
            startIcon={<PlayArrow />}
            size="small"
            sx={{ height: '32px' }}
         >
            Run All
         </Button>
      </SQLView>
   );
}
