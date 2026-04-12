// QueryBrew (c) 2025
import Autocomplete from '@mui/material/Autocomplete';
import TextField from '@mui/material/TextField';
import { Query } from '../Api';

interface QuerySelectionProps {
   name: string;
   queries: Query[];
   setSqlQuery: (query: string) => void;
   onSelectQuery?: (query: string) => void;
   selectedQueryName?: string | null;
}

/**
 * Component that renders an autocomplete field to load existing benchmark queries
 */
export default function QuerySelection({
   name,
   queries,
   setSqlQuery,
   onSelectQuery,
   selectedQueryName,
}: QuerySelectionProps) {
   const handleSelectQuery = (_event: any, value: Query | null) => {
      if (value) {
         setSqlQuery(value.sql);
         // Trigger run after selection if callback provided
         if (onSelectQuery) {
            // Use setTimeout to allow state to update before running
            setTimeout(() => onSelectQuery(value.sql), 0);
         }
      }
   };

   // Find the currently selected query object
   const selectedQuery = queries.find((q) => q.name === selectedQueryName) || null;

   return (
      <Autocomplete
         options={queries}
         value={selectedQuery}
         getOptionLabel={(option) => option.name}
         onChange={handleSelectQuery}
         size="small"
         sx={{ width: '180px', marginLeft: 'auto' }}
         renderInput={(params) => (
            <TextField
               {...params}
               label={`${name} Queries`}
               variant="outlined"
               size="small"
               InputProps={{
                  ...params.InputProps,
                  sx: { height: '32px', fontSize: '0.875rem' },
               }}
               InputLabelProps={{
                  sx: {
                     fontSize: '0.875rem',
                     top: '-4px',
                     '&.MuiInputLabel-shrink': { top: '2px' },
                  },
               }}
            />
         )}
      />
   );
}
