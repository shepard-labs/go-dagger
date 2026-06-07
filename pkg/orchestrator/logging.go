package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/shepard-labs/go-dagger/pkg/persistence"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type loggerContextKey struct{}

type logInserter interface {
	Insert(context.Context, uuid.UUID, *uuid.UUID, persistence.LogLevel, string, json.RawMessage) (*persistence.TaskLog, error)
}

func LoggerFromContext(ctx context.Context) *zap.Logger {
	logger, _ := ctx.Value(loggerContextKey{}).(*zap.Logger)
	if logger == nil {
		return zap.NewNop()
	}
	return logger
}

func contextWithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	if logger == nil {
		logger = zap.NewNop()
	}
	return context.WithValue(ctx, loggerContextKey{}, logger)
}

func (o *Orchestrator[S]) dagLogger(runID uuid.UUID, dagName string) *zap.Logger {
	return o.logger.With(zap.String("dag_run_id", runID.String()), zap.String("dag_name", dagName))
}

func (o *Orchestrator[S]) taskLogger(runID, taskRunID uuid.UUID, dagName, taskName string, attempt int) *zap.Logger {
	return o.dagLogger(runID, dagName).With(
		zap.String("task_run_id", taskRunID.String()),
		zap.String("task_name", taskName),
		zap.Int("attempt", attempt),
	)
}

func newFanOutLogger(base *zap.Logger, store logInserter, stdout, stderr io.Writer) *zap.Logger {
	if stdout == nil {
		stdout = os.Stdout
	}
	if stderr == nil {
		stderr = os.Stderr
	}
	return zap.New(newLogCore(stdout, stderr, store), zap.AddCallerSkip(1))
}

type logCore struct {
	encoder zapcore.Encoder
	stdout  io.Writer
	stderr  io.Writer
	store   logInserter
	fields  []zapcore.Field
}

func newLogCore(stdout, stderr io.Writer, store logInserter) zapcore.Core {
	config := zap.NewProductionEncoderConfig()
	config.TimeKey = "ts"
	config.EncodeTime = zapcore.ISO8601TimeEncoder
	config.EncodeLevel = zapcore.LowercaseLevelEncoder
	return &logCore{encoder: zapcore.NewJSONEncoder(config), stdout: stdout, stderr: stderr, store: store}
}

func (c *logCore) Enabled(level zapcore.Level) bool {
	return level >= zapcore.DebugLevel && level <= zapcore.ErrorLevel
}

func (c *logCore) With(fields []zapcore.Field) zapcore.Core {
	clone := *c
	clone.fields = append(append([]zapcore.Field(nil), c.fields...), fields...)
	return &clone
}

func (c *logCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c *logCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	allFields := append(append([]zapcore.Field(nil), c.fields...), fields...)
	redactedEntry := entry
	redactedEntry.Message = RedactLogValue(entry.Message)
	encodedFields := redactZapFields(allFields)
	buf, err := c.encoder.Clone().EncodeEntry(redactedEntry, encodedFields)
	if err != nil {
		fmt.Fprintf(c.stderr, "log encode failure: %s\n", RedactLogValue(err.Error()))
		return nil
	}
	_, _ = c.stdout.Write(buf.Bytes())
	buf.Free()
	if c.store == nil {
		return nil
	}
	if err := c.insertLog(redactedEntry, encodedFields); err != nil {
		fmt.Fprintf(c.stderr, "log persistence failure: %s\n", RedactLogValue(err.Error()))
	}
	return nil
}

func (c *logCore) Sync() error { return nil }

func (c *logCore) insertLog(entry zapcore.Entry, fields []zapcore.Field) error {
	fieldMap, err := zapFieldsToMap(fields)
	if err != nil {
		return err
	}
	dagRunID, err := uuid.Parse(stringFromMap(fieldMap, "dag_run_id"))
	if err != nil {
		return fmt.Errorf("missing or invalid dag_run_id")
	}
	var taskRunID *uuid.UUID
	if rawTaskRunID := stringFromMap(fieldMap, "task_run_id"); rawTaskRunID != "" {
		parsed, err := uuid.Parse(rawTaskRunID)
		if err != nil {
			return fmt.Errorf("invalid task_run_id")
		}
		taskRunID = &parsed
	}
	data, err := json.Marshal(fieldMap)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.store.Insert(ctx, dagRunID, taskRunID, zapLevelToLogLevel(entry.Level), entry.Message, data)
	return err
}

func zapFieldsToMap(fields []zapcore.Field) (map[string]any, error) {
	values := make(map[string]any, len(fields))
	for _, field := range fields {
		switch field.Type {
		case zapcore.StringType:
			values[field.Key] = field.String
		case zapcore.BoolType:
			values[field.Key] = field.Integer == 1
		case zapcore.Int64Type, zapcore.Int32Type, zapcore.Int16Type, zapcore.Int8Type:
			values[field.Key] = field.Integer
		case zapcore.Uint64Type, zapcore.Uint32Type, zapcore.Uint16Type, zapcore.Uint8Type, zapcore.UintptrType:
			values[field.Key] = uint64(field.Integer)
		case zapcore.Float64Type:
			values[field.Key] = field.Interface
		case zapcore.Float32Type:
			values[field.Key] = field.Interface
		case zapcore.ErrorType, zapcore.StringerType:
			values[field.Key] = fmt.Sprint(field.Interface)
		case zapcore.ReflectType:
			values[field.Key] = field.Interface
		default:
			encoder := zapcore.NewMapObjectEncoder()
			field.AddTo(encoder)
			if value, ok := encoder.Fields[field.Key]; ok {
				values[field.Key] = value
			}
		}
	}
	return values, nil
}

func stringFromMap(values map[string]any, key string) string {
	value, ok := values[key]
	if !ok || value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func zapLevelToLogLevel(level zapcore.Level) persistence.LogLevel {
	switch level {
	case zapcore.DebugLevel:
		return persistence.LogLevelDebug
	case zapcore.WarnLevel:
		return persistence.LogLevelWarn
	case zapcore.ErrorLevel, zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		return persistence.LogLevelError
	default:
		return persistence.LogLevelInfo
	}
}

func redactZapFields(fields []zapcore.Field) []zapcore.Field {
	redacted := make([]zapcore.Field, 0, len(fields))
	for _, field := range fields {
		redacted = append(redacted, redactZapField(field))
	}
	return redacted
}

func redactZapField(field zapcore.Field) zapcore.Field {
	switch field.Type {
	case zapcore.StringType:
		field = zap.String(field.Key, RedactLogValue(field.String))
	case zapcore.ErrorType:
		if field.Interface != nil {
			field = zap.String(field.Key, RedactLogValue(fmt.Sprint(field.Interface)))
		}
	case zapcore.StringerType:
		if field.Interface != nil {
			field = zap.String(field.Key, RedactLogValue(fmt.Sprint(field.Interface)))
		}
	case zapcore.ReflectType, zapcore.NamespaceType:
		if field.Interface != nil {
			field.Interface = redactValue(field.Interface)
		}
	}
	return field
}

func redactValue(value any) any {
	data, err := json.Marshal(value)
	if err != nil {
		return RedactLogValue(fmt.Sprint(value))
	}
	var decoded any
	if err := json.Unmarshal([]byte(RedactLogValue(string(data))), &decoded); err != nil {
		return RedactLogValue(string(data))
	}
	return decoded
}

func RedactLogValue(input string) string {
	redacted := input
	if strings.Contains(input, "postgres://") || strings.Contains(input, "postgresql://") {
		redacted = persistence.RedactDSN(input)
	}
	for _, key := range []string{"password", "passwd", "pwd", "secret"} {
		redacted = redactKeyValue(redacted, key)
	}
	return redacted
}

func redactKeyValue(input, key string) string {
	searchFrom := 0
	for {
		if searchFrom >= len(input) {
			return input
		}
		lower := strings.ToLower(input[searchFrom:])
		idx := strings.Index(lower, key+"=")
		if idx < 0 {
			return input
		}
		idx += searchFrom
		start := idx + len(key) + 1
		end := start
		for end < len(input) && input[end] != ' ' && input[end] != '&' && input[end] != ';' {
			end++
		}
		input = input[:start] + "[redacted]" + input[end:]
		searchFrom = start + len("[redacted]")
	}
}

type memoryLogStore struct {
	logs []persistence.TaskLog
	err  error
}

func (m *memoryLogStore) Insert(_ context.Context, dagRunID uuid.UUID, taskRunID *uuid.UUID, level persistence.LogLevel, message string, fields json.RawMessage) (*persistence.TaskLog, error) {
	if m.err != nil {
		return nil, m.err
	}
	log := persistence.TaskLog{ID: persistence.NewTaskLogID(), DAGRunID: dagRunID, TaskRunID: taskRunID, Level: level, Message: message, Fields: fields, CreatedAt: time.Now()}
	m.logs = append(m.logs, log)
	return &log, nil
}
