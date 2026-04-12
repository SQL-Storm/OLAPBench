// QueryBrew (c) 2025
import Stack from '@mui/material/Stack';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import IconButton from '@mui/material/IconButton';
import Tooltip from '@mui/material/Tooltip';
import Typography from '@mui/material/Typography';
import TextField from '@mui/material/TextField';
import AddIcon from '@mui/icons-material/Add';
import Brightness4Icon from '@mui/icons-material/Brightness4';
import Brightness7Icon from '@mui/icons-material/Brightness7';

interface TitleBarProps {
   handleConnect: () => void;
   isLoading?: boolean;
   isConnected: boolean;
   hostname: string;
   setHostname: (hostname: string) => void;
   port: string;
   setPort: (port: string) => void;
   timeout: string;
   setTimeout: (timeout: string) => void;
   resultLimit: string;
   setResultLimit: (resultLimit: string) => void;
   onAddOutput?: () => void;
   darkMode: boolean;
   toggleDarkMode: () => void;
}

/**
 * Title bar component that displays the application title and connection controls
 */
export default function TitleBar({
   handleConnect,
   isLoading,
   isConnected,
   hostname,
   setHostname,
   port,
   setPort,
   timeout,
   setTimeout,
   resultLimit,
   setResultLimit,
   onAddOutput,
   darkMode,
   toggleDarkMode,
}: TitleBarProps) {
   return (
      <Stack
         direction="row"
         spacing={2}
         sx={{
            height: '100%',
            pl: 2,
            pr: 2,
            alignItems: 'center',
            backgroundColor: '#8882',
         }}
      >
         <Typography noWrap sx={{ fontSize: 28 }}>
            <strong>🍺 QueryBrew</strong>
         </Typography>
         <Box sx={{ width: 24, flexShrink: 0 }} />
         <TextField
            label="Host"
            value={hostname}
            onChange={(e) => setHostname(e.target.value)}
            size="small"
            sx={{ width: '180px' }}
            disabled={isLoading}
         />
         <TextField
            label="Port"
            value={port}
            onChange={(e) => setPort(e.target.value)}
            size="small"
            sx={{ width: '100px' }}
            type="number"
            disabled={isLoading}
         />
         <TextField
            label="Timeout (s)"
            value={timeout}
            onChange={(e) => setTimeout(e.target.value)}
            size="small"
            sx={{ width: '120px' }}
            type="number"
            inputProps={{ min: 1, step: 1 }}
            disabled={isLoading}
         />
         <TextField
            label="Result Limit"
            value={resultLimit}
            onChange={(e) => setResultLimit(e.target.value)}
            size="small"
            sx={{ width: '130px' }}
            type="number"
            inputProps={{ min: 1, step: 1 }}
            disabled={isLoading}
         />
         <Button
            sx={{ height: '39px', lineHeight: 1 }}
            variant="contained"
            color={isConnected ? 'success' : 'primary'}
            onClick={handleConnect}
            disabled={isLoading}
         >
            {isConnected ? '✓ Connected' : 'Connect'}
         </Button>
         <Box sx={{ flexGrow: 1 }} />
         <Tooltip title={darkMode ? 'Switch to Light Mode' : 'Switch to Dark Mode'}>
            <IconButton onClick={toggleDarkMode} color="inherit" size="small">
               {darkMode ? <Brightness7Icon /> : <Brightness4Icon />}
            </IconButton>
         </Tooltip>
         {onAddOutput && (
            <Tooltip title="Add Output View">
               <Button
                  onClick={onAddOutput}
                  variant="contained"
                  color="secondary"
                  size="small"
                  startIcon={<AddIcon />}
                  sx={{ height: '32px', fontSize: '0.875rem', lineHeight: 1 }}
               >
                  Add
               </Button>
            </Tooltip>
         )}
      </Stack>
   );
}
