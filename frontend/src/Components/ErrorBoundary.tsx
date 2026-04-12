// QueryBrew (c) 2025
import React from 'react';
import Box from '@mui/material/Box';
import Typography from '@mui/material/Typography';
import Button from '@mui/material/Button';

interface ErrorBoundaryProps {
   children: React.ReactNode;
}

interface ErrorBoundaryState {
   hasError: boolean;
   error: Error | null;
}

/**
 * Error Boundary component to catch and display React errors gracefully
 */
class ErrorBoundary extends React.Component<ErrorBoundaryProps, ErrorBoundaryState> {
   constructor(props: ErrorBoundaryProps) {
      super(props);
      this.state = { hasError: false, error: null };
   }

   static getDerivedStateFromError(error: Error): ErrorBoundaryState {
      return { hasError: true, error };
   }

   componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
      console.error('Error boundary caught:', error, errorInfo);
   }

   handleReset = () => {
      this.setState({ hasError: false, error: null });
      window.location.reload();
   };

   render() {
      if (this.state.hasError) {
         return (
            <Box
               sx={{
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  justifyContent: 'center',
                  height: '100vh',
                  gap: 2,
                  padding: 4,
               }}
            >
               <Typography variant="h4" color="error">
                  Oops! Something went wrong
               </Typography>
               <Typography variant="body1" color="text.secondary">
                  {this.state.error?.message || 'An unexpected error occurred'}
               </Typography>
               <Button variant="contained" onClick={this.handleReset}>
                  Reload Application
               </Button>
            </Box>
         );
      }

      return this.props.children;
   }
}

export default ErrorBoundary;
