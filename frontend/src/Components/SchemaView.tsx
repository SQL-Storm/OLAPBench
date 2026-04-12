// QueryBrew (c) 2025
import Select, { SelectChangeEvent } from '@mui/material/Select';
import MenuItem from '@mui/material/MenuItem';
import SQLView from './SQLView';

interface SchemaViewProps {
   schema: string;
   datasets?: string[];
   activeDataset?: string;
   onDatasetChange?: (dataset: string) => void;
}

/**
 * Schema view component (read-only) that displays the database schema.
 * Shows a dataset selector when multiple datasets are available.
 */
export default function SchemaView({
   schema,
   datasets,
   activeDataset,
   onDatasetChange,
}: SchemaViewProps) {
   const showSelector = datasets && datasets.length > 1 && activeDataset && onDatasetChange;

   return (
      <SQLView title="Schema" value={schema} setValue={() => {}} readOnly={true}>
         {showSelector && (
            <Select
               value={activeDataset}
               onChange={(e: SelectChangeEvent) => onDatasetChange(e.target.value)}
               size="small"
               sx={{ fontSize: '0.8rem', height: '28px', minWidth: '100px' }}
            >
               {datasets.map((d) => (
                  <MenuItem key={d} value={d} sx={{ fontSize: '0.8rem' }}>
                     {d}
                  </MenuItem>
               ))}
            </Select>
         )}
      </SQLView>
   );
}
