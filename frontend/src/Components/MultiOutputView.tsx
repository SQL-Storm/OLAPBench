// QueryBrew (c) 2025
import Box from '@mui/material/Box';
import OptimizedQueryTab from './OptimizedQueryTab';
import ResizablePanels from './ResizablePanels';
import { ActiveDbms, QueryResponse } from '../Api';
import { PlanResult } from './QueryResultView';

interface OutputResult {
   id: string;
   result: string;
   error: string | null;
   dbms: string;
   editedValue?: string;
   originalQuery?: string;
   optimizedQuery?: string | null;
   queryResult?: QueryResponse | null;
   viewMode?: 'table' | 'plan';
   queryPlan?: PlanResult | null;
   autoRunEnabled?: boolean;
   autoOptimize?: boolean;
}

interface MultiOutputViewProps {
   outputs: OutputResult[];
   onRun: (
      outputId: string,
      dbms: string,
      queryOverride?: string
   ) => Promise<{ success: boolean; query: string }>;
   onOptimize: (outputId: string, dbms: string) => void;
   onRevertOptimizedQuery: (outputId: string) => void;
   onCloseOutput: (outputId: string) => void;
   onEditOutput: (outputId: string, editedValue: string) => void;
   onDbmsChange: (outputId: string, dbms: string) => void;
   onViewModeChange: (outputId: string, mode: 'table' | 'plan') => void;
   onPlanFetched: (outputId: string, plan: PlanResult | null) => void;
   onToggleAutoRun: (outputId: string, enabled: boolean) => void;
   onToggleAutoOptimize: (outputId: string, enabled: boolean) => void;
   isLoading: boolean;
   activeDbms: ActiveDbms[];
   hostname: string;
   port: string;
   timeout: number;
}

/**
 * Container for multiple optimized query output views
 * Uses ResizablePanels for horizontal resizing between output views
 */
export default function MultiOutputView({
   outputs,
   onRun,
   onOptimize,
   onRevertOptimizedQuery,
   onCloseOutput,
   onEditOutput,
   onDbmsChange,
   onViewModeChange,
   onPlanFetched,
   onToggleAutoRun,
   onToggleAutoOptimize,
   isLoading,
   activeDbms,
   hostname,
   port,
   timeout,
}: MultiOutputViewProps) {
   if (outputs.length === 0) {
      return null;
   }

   // Single output - no resizing needed
   if (outputs.length === 1) {
      const output = outputs[0];
      return (
         <Box sx={{ height: '100%', width: '100%' }}>
            <OptimizedQueryTab
               id={output.id}
               result={output.result}
               editedValue={output.editedValue}
               queryResult={output.queryResult}
               onRun={(dbms) => onRun(output.id, dbms)}
               onOptimize={(dbms) => onOptimize(output.id, dbms)}
               onEditValue={(value) => onEditOutput(output.id, value)}
               onDbmsChange={(dbms) => onDbmsChange(output.id, dbms)}
               viewMode={output.viewMode}
               onViewModeChange={(mode) => onViewModeChange(output.id, mode)}
               queryPlan={output.queryPlan}
               onPlanFetched={(plan) => onPlanFetched(output.id, plan)}
               autoRunEnabled={output.autoRunEnabled !== false}
               onToggleAutoRun={(enabled) => onToggleAutoRun(output.id, enabled)}
               autoOptimize={output.autoOptimize === true}
               onToggleAutoOptimize={(enabled) => onToggleAutoOptimize(output.id, enabled)}
               originalQuery={output.originalQuery}
               optimizedQuery={output.optimizedQuery}
               onRevertOptimizedQuery={() => onRevertOptimizedQuery(output.id)}
               isLoading={isLoading}
               showCloseButton={false}
               initialDbms={output.dbms}
               activeDbms={activeDbms}
               hostname={hostname}
               port={port}
               timeout={timeout}
            />
         </Box>
      );
   }

   // Recursive function to build nested ResizablePanels for multiple outputs
   // Use outputs.length in key to force re-render and reset sizes when count changes
   const buildPanels = (startIndex: number): React.ReactElement => {
      const output = outputs[startIndex];

      // Last panel - no more nesting needed
      if (startIndex === outputs.length - 1) {
         return (
            <OptimizedQueryTab
               key={output.id}
               id={output.id}
               result={output.result}
               editedValue={output.editedValue}
               queryResult={output.queryResult}
               onClose={() => onCloseOutput(output.id)}
               onRun={(dbms) => onRun(output.id, dbms)}
               onOptimize={(dbms) => onOptimize(output.id, dbms)}
               onEditValue={(value) => onEditOutput(output.id, value)}
               onDbmsChange={(dbms) => onDbmsChange(output.id, dbms)}
               viewMode={output.viewMode}
               onViewModeChange={(mode) => onViewModeChange(output.id, mode)}
               queryPlan={output.queryPlan}
               onPlanFetched={(plan) => onPlanFetched(output.id, plan)}
               autoRunEnabled={output.autoRunEnabled !== false}
               onToggleAutoRun={(enabled) => onToggleAutoRun(output.id, enabled)}
               autoOptimize={output.autoOptimize === true}
               onToggleAutoOptimize={(enabled) => onToggleAutoOptimize(output.id, enabled)}
               originalQuery={output.originalQuery}
               optimizedQuery={output.optimizedQuery}
               onRevertOptimizedQuery={() => onRevertOptimizedQuery(output.id)}
               isLoading={isLoading}
               showCloseButton={true}
               initialDbms={output.dbms}
               activeDbms={activeDbms}
               hostname={hostname}
               port={port}
               timeout={timeout}
            />
         );
      }

      // Calculate initial size for uniform distribution
      const remainingPanels = outputs.length - startIndex;
      const panelSize = (1 / remainingPanels) * 100;

      return (
         <ResizablePanels
            key={`panel-${startIndex}-${outputs.length}`}
            direction="horizontal"
            initialSize={panelSize}
            minSize={10}
         >
            <OptimizedQueryTab
               key={output.id}
               id={output.id}
               result={output.result}
               editedValue={output.editedValue}
               queryResult={output.queryResult}
               onClose={() => onCloseOutput(output.id)}
               onRun={(dbms) => onRun(output.id, dbms)}
               onOptimize={(dbms) => onOptimize(output.id, dbms)}
               onEditValue={(value) => onEditOutput(output.id, value)}
               onDbmsChange={(dbms) => onDbmsChange(output.id, dbms)}
               viewMode={output.viewMode}
               onViewModeChange={(mode) => onViewModeChange(output.id, mode)}
               queryPlan={output.queryPlan}
               onPlanFetched={(plan) => onPlanFetched(output.id, plan)}
               autoRunEnabled={output.autoRunEnabled !== false}
               onToggleAutoRun={(enabled) => onToggleAutoRun(output.id, enabled)}
               autoOptimize={output.autoOptimize === true}
               onToggleAutoOptimize={(enabled) => onToggleAutoOptimize(output.id, enabled)}
               originalQuery={output.originalQuery}
               optimizedQuery={output.optimizedQuery}
               onRevertOptimizedQuery={() => onRevertOptimizedQuery(output.id)}
               isLoading={isLoading}
               showCloseButton={true}
               initialDbms={output.dbms}
               activeDbms={activeDbms}
               hostname={hostname}
               port={port}
               timeout={timeout}
            />
            {buildPanels(startIndex + 1)}
         </ResizablePanels>
      );
   };

   return <Box sx={{ height: '100%', width: '100%' }}>{buildPanels(0)}</Box>;
}
