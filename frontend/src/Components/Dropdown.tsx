// QueryBrew (c) 2025
import MenuItem from '@mui/material/MenuItem';
import FormControl from '@mui/material/FormControl';
import Select, { SelectChangeEvent } from '@mui/material/Select';
import { ActiveDbms } from '../Api';

interface DropdownProps {
   value: string;
   setValue: (value: string) => void;
   options: ActiveDbms[];
}

/**
 * Dropdown component for selecting target database
 * Styled as a title/header element but still interactive
 */
export default function Dropdown({ value, setValue, options }: DropdownProps) {
   return (
      <FormControl
         size="small"
         variant="standard"
         sx={{ display: 'flex', justifyContent: 'center' }}
      >
         <Select
            id="target-dbms-select"
            value={value}
            onChange={(event: SelectChangeEvent) => setValue(event.target.value)}
            disableUnderline
            MenuProps={{
               anchorOrigin: {
                  vertical: 'bottom',
                  horizontal: 'left',
               },
               transformOrigin: {
                  vertical: 'top',
                  horizontal: 'left',
               },
            }}
            sx={{
               fontWeight: 'bold',
               fontSize: '1rem',
               cursor: 'pointer',
               '& .MuiSelect-select': {
                  paddingTop: 0,
                  paddingBottom: 0,
                  paddingRight: '28px !important',
                  display: 'flex',
                  alignItems: 'center',
               },
               '& .MuiSelect-icon': {
                  right: '4px',
               },
               '&:hover': {
                  backgroundColor: 'rgba(0, 0, 0, 0.04)',
                  borderRadius: 1,
               },
            }}
         >
            {options.map((option) => (
               <MenuItem key={option.id} value={option.id}>
                  {option.title}
               </MenuItem>
            ))}
         </Select>
      </FormControl>
   );
}
