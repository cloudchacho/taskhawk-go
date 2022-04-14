package taskhawk

import "context"

// GetLoggerFunc returns the logger object
type GetLoggerFunc func(ctx context.Context) Logger
