// QueryBrew (c) 2025

/**
 * Application constants
 */
export const DEFAULT_OUTPUT_MESSAGE = '-- Your optimized query will be displayed here';
export const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

/**
 * Grid layout configuration
 */
export const LAYOUT = {
   TITLE_BAR_HEIGHT: '72px',
   GRID_TEMPLATE: '"top top" 72px "left right" 1fr / 1fr 1fr',
   BORDER_STYLE: '0.5px solid #8888',
};
