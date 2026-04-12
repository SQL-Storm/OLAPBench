// QueryBrew (c) 2025
import { useState, useRef, useEffect, ReactNode } from 'react';
import Box from '@mui/material/Box';

interface ResizablePanelsProps {
   direction: 'horizontal' | 'vertical';
   initialSize?: number;
   minSize?: number;
   children: [ReactNode, ReactNode];
   borderStyle?: string;
}

/**
 * Resizable panels component with draggable splitter
 */
export default function ResizablePanels({
   direction,
   initialSize = 50,
   minSize = 10,
   children,
   borderStyle = '0.5px solid #8888',
}: ResizablePanelsProps) {
   const [size, setSize] = useState(initialSize);
   const [isDragging, setIsDragging] = useState(false);
   const containerRef = useRef<HTMLDivElement>(null);

   const handleMouseDown = (e: React.MouseEvent) => {
      e.preventDefault();
      setIsDragging(true);
   };

   useEffect(() => {
      const handleMouseMove = (e: MouseEvent) => {
         if (!isDragging || !containerRef.current) return;

         const rect = containerRef.current.getBoundingClientRect();
         let newSize: number;

         if (direction === 'horizontal') {
            newSize = ((e.clientX - rect.left) / rect.width) * 100;
         } else {
            newSize = ((e.clientY - rect.top) / rect.height) * 100;
         }

         newSize = Math.max(minSize, Math.min(100 - minSize, newSize));
         setSize(newSize);
      };

      const handleMouseUp = () => {
         setIsDragging(false);
      };

      if (isDragging) {
         document.addEventListener('mousemove', handleMouseMove);
         document.addEventListener('mouseup', handleMouseUp);
      }

      return () => {
         document.removeEventListener('mousemove', handleMouseMove);
         document.removeEventListener('mouseup', handleMouseUp);
      };
   }, [isDragging, direction, minSize]);

   const flexDirection = direction === 'horizontal' ? 'row' : 'column';
   const splitterCursor = direction === 'horizontal' ? 'col-resize' : 'row-resize';
   const splitterSize = '1px';

   return (
      <Box
         ref={containerRef}
         sx={{
            display: 'flex',
            flexDirection,
            height: '100%',
            width: '100%',
            overflow: 'hidden',
         }}
      >
         <Box
            sx={{
               flex: `0 0 ${size}%`,
               overflow: 'hidden',
               ...(direction === 'horizontal'
                  ? { borderRight: borderStyle }
                  : { borderBottom: borderStyle }),
            }}
         >
            {children[0]}
         </Box>
         <Box
            onMouseDown={handleMouseDown}
            sx={{
               flex: `0 0 ${splitterSize}`,
               cursor: splitterCursor,
               backgroundColor: isDragging ? '#8888' : 'transparent',
               '&:hover': {
                  backgroundColor: '#8886',
               },
               userSelect: 'none',
               zIndex: 10,
               position: 'relative',
               '&::before': {
                  content: '""',
                  position: 'absolute',
                  top: 0,
                  left: '-4px',
                  right: '-4px',
                  bottom: 0,
                  ...(direction === 'vertical' && {
                     left: 0,
                     right: 0,
                     top: '-4px',
                     bottom: '-4px',
                  }),
               },
            }}
         />
         <Box
            sx={{
               flex: 1,
               overflow: 'hidden',
            }}
         >
            {children[1]}
         </Box>
      </Box>
   );
}
