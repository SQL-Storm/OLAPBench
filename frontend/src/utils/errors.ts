// QueryBrew (c) 2025

/**
 * Extract a human-readable message from an unknown thrown value, falling back to the
 * provided default when no usable message is present.
 */
export function errorMessage(error: unknown, fallback = 'An unexpected error occurred'): string {
   if (error instanceof Error && error.message) {
      return error.message;
   }
   if (typeof error === 'string' && error) {
      return error;
   }
   return fallback;
}
