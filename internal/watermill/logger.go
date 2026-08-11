package watermill

import (
	"log/slog"
	"sort"

	wm "github.com/ThreeDotsLabs/watermill"
)

// slogAdapter adapts a *slog.Logger to watermill's LoggerAdapter.
//
// Without it watermill logs through its own NewStdLogger, so its
// "Handler returned error" line — operationally the most interesting event
// the router produces — lands in the stdlib logger in watermill's own
// format, in a different stream from everything else the library emits.
type slogAdapter struct {
	log *slog.Logger
}

// newLoggerAdapter wraps log for watermill. A nil log falls back to slog's
// default, matching Config.Validate.
func newLoggerAdapter(log *slog.Logger) wm.LoggerAdapter {
	if log == nil {
		log = slog.Default()
	}
	return slogAdapter{log: log}
}

func (a slogAdapter) Error(msg string, err error, fields wm.LogFields) {
	a.log.Error(logPrefix+msg, append(logAttrs(fields), slog.Any("error", err))...)
}

func (a slogAdapter) Info(msg string, fields wm.LogFields) {
	a.log.Info(logPrefix+msg, logAttrs(fields)...)
}

func (a slogAdapter) Debug(msg string, fields wm.LogFields) {
	a.log.Debug(logPrefix+msg, logAttrs(fields)...)
}

// Trace maps to debug: slog has no level below debug, and watermill's trace
// output is debug-grade detail.
func (a slogAdapter) Trace(msg string, fields wm.LogFields) {
	a.log.Debug(logPrefix+msg, logAttrs(fields)...)
}

func (a slogAdapter) With(fields wm.LogFields) wm.LoggerAdapter {
	return slogAdapter{log: a.log.With(logAttrs(fields)...)}
}

// logPrefix marks a line as watermill's own rather than this library's, so
// the two are distinguishable once they share a logger.
const logPrefix = "watermill: "

// logAttrs converts watermill's LogFields to slog attributes, sorted by key
// so a given log line is byte-identical run to run (LogFields is a map).
func logAttrs(fields wm.LogFields) []any {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	attrs := make([]any, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, slog.Any(k, fields[k]))
	}
	return attrs
}
