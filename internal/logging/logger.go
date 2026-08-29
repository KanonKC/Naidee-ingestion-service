package logging

import (
	"log/slog"

	"event/ingestion-service/internal/libs"

	"github.com/google/uuid"
)

// Layer mirrors the Layer enum in blaze-backend.
type Layer string

const (
	LayerController Layer = "controller"
	LayerService    Layer = "service"
	LayerMiddleware Layer = "middleware"
	LayerRepository Layer = "repository"
	LayerProvider   Layer = "provider"
	LayerCron       Layer = "cron"
	LayerOther      Layer = "other"
)

// Meta mirrors the LogMeta interface in blaze-backend.
type Meta struct {
	Message string
	Data    any
	Error   error
}

type SetContextOptions struct {
	Silent bool
}

// TLogger is the structured logger used by every controller, service,
// repository and provider in this service.
//
// Unlike its TypeScript counterpart, SetContext and With return a *copy* rather
// than mutating the receiver. The ingestion worker pool logs from several
// goroutines at once, and a shared mutable context/transaction id would be both
// a data race and a source of mislabelled log lines.
type TLogger struct {
	layer         Layer
	context       string
	transactionID string
	fields        []slog.Attr
}

func New(layer Layer) *TLogger {
	return &TLogger{layer: layer}
}

// SetContext returns a logger scoped to a "layer.domain.action" context with a
// fresh transaction id. Call it as the first line of every method.
func (l *TLogger) SetContext(context string, options ...SetContextOptions) *TLogger {
	next := l.clone()
	next.context = context
	next.transactionID = uuid.NewString()

	silent := len(options) > 0 && options[0].Silent
	if !silent {
		next.Info(Meta{Message: "Init: " + context})
	}
	return next
}

// With attaches a field that sticks to every subsequent line — this is how
// run_id ends up on every log line of an ingestion run.
func (l *TLogger) With(key string, value any) *TLogger {
	next := l.clone()
	next.fields = append(next.fields, slog.Any(key, value))
	return next
}

func (l *TLogger) clone() *TLogger {
	fields := make([]slog.Attr, len(l.fields))
	copy(fields, l.fields)
	return &TLogger{
		layer:         l.layer,
		context:       l.context,
		transactionID: l.transactionID,
		fields:        fields,
	}
}

func (l *TLogger) createPayload(meta Meta) []any {
	attrs := make([]any, 0, len(l.fields)+5)
	attrs = append(attrs,
		slog.String("layer", string(l.layer)),
		slog.String("context", l.context),
		slog.String("transaction_id", l.transactionID),
	)
	for _, field := range l.fields {
		attrs = append(attrs, field)
	}
	if meta.Data != nil {
		attrs = append(attrs, slog.Any("data", meta.Data))
	}
	if meta.Error != nil {
		// Stored as a string: most error types marshal to "{}" as JSON.
		attrs = append(attrs, slog.String("error", meta.Error.Error()))
	}
	return attrs
}

func (l *TLogger) Info(meta Meta) {
	libs.Logger.Info(meta.Message, l.createPayload(meta)...)
}

func (l *TLogger) Warn(meta Meta) {
	libs.Logger.Warn(meta.Message, l.createPayload(meta)...)
}

func (l *TLogger) Error(meta Meta) {
	libs.Logger.Error(meta.Message, l.createPayload(meta)...)
}

func (l *TLogger) Debug(meta Meta) {
	libs.Logger.Debug(meta.Message, l.createPayload(meta)...)
}
