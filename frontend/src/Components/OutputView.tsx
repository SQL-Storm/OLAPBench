// QueryBrew (c) 2025
import SQLView from './SQLView';

interface OutputViewProps {
   result: string;
   error?: string | null;
}

/**
 * Output view component that displays the optimized SQL query result
 */
export default function OutputView({ result, error }: OutputViewProps) {
   const displayValue = error ? `-- Error occurred:\n-- ${error}` : result;

   return (
      <SQLView title="Optimized Query" value={displayValue} setValue={() => {}} readOnly={true} />
   );
}
