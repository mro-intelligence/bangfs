package bangutil

import (
	"fmt"
	"log"
	"os"
	"sync"
	"syscall"
	"time"
)

// Tracer provides tracing for filesystem operations
type Tracer struct {
	enabled bool
	logger  *log.Logger
	file    *os.File
	mu      sync.Mutex
}

var (
	globalTracer *Tracer
	once         sync.Once
)

// GetTracer returns the global tracer instance
func GetTracer() *Tracer {
	once.Do(func() {
		globalTracer = &Tracer{
			enabled: false,
			logger:  log.New(os.Stderr, "[TRACE] ", log.Ltime|log.Lmicroseconds),
		}
	})
	return globalTracer
}

// SetOutputFile redirects trace output to a file instead of stderr.
func (t *Tracer) SetOutputFile(path string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	t.file = f
	t.logger.SetOutput(f)
	return nil
}

// CloseOutput closes the trace output file if one was set.
func (t *Tracer) CloseOutput() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.file != nil {
		t.file.Close()
		t.file = nil
	}
}

// Enable turns on tracing
func (t *Tracer) Enable() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enabled = true
	t.logger.Println("Tracing enabled")
}

// Disable turns off tracing
func (t *Tracer) Disable() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.enabled = false
}

// IsEnabled returns whether tracing is enabled
func (t *Tracer) IsEnabled() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.enabled
}

// Op traces a filesystem operation
func (t *Tracer) Op(op string, inum uint64, name string) *TraceOp {
	if !t.IsEnabled() {
		return &TraceOp{tracer: nil}
	}
	return &TraceOp{
		tracer: t,
		op:     op,
		inum:   inum,
		name:   name,
		start:  time.Now(),
	}
}

// TraceOp represents a traced operation
type TraceOp struct {
	tracer *Tracer
	op     string
	inum   uint64
	name   string
	start  time.Time
}

// SetName sets the name attribute of the op
func (o *TraceOp) SetName(name string) {
	o.name = name
}

// Done marks the operation as complete with success
func (o *TraceOp) Done() {
	if o.tracer == nil {
		return
	}
	elapsed := time.Since(o.start)
	o.tracer.logger.Printf("%s(inum=%d, name=%q) OK [%v]", o.op, o.inum, o.name, elapsed)
}

// Error marks the operation as failed with an error
func (o *TraceOp) Error(err error) {
	if o.tracer == nil {
		return
	}
	elapsed := time.Since(o.start)
	o.tracer.logger.Printf("%s(inum=%d, name=%q) ERROR: %v [%v]", o.op, o.inum, o.name, err, elapsed)
}

// Errorf marks the operation as failed with printf-style formatting
func (o *TraceOp) Errorf(format string, args ...interface{}) {
	o.Error(fmt.Errorf(format, args...))
}

// Debug marks a debug message
func (o *TraceOp) Debug(info string) {
	if o.tracer == nil {
		return
	}
	elapsed := time.Since(o.start)
	o.tracer.logger.Printf("%s(inum=%d, name=%q) DEBUG: %v [%v]", o.op, o.inum, o.name, info, elapsed)
}

// Debugf marks a debug message with printf-style formatting
func (o *TraceOp) Debugf(format string, args ...interface{}) {
	if o.tracer == nil {
		return
	}
	o.Debug(fmt.Sprintf(format, args...))
}

// Errno marks the operation as failed with a syscall errno
func (o *TraceOp) Errno(errno syscall.Errno) {
	if o.tracer == nil {
		return
	}
	elapsed := time.Since(o.start)
	errName := ErrnoName(errno)
	o.tracer.logger.Printf("%s(inum=%d, name=%q) %s [%v]", o.op, o.inum, o.name, errName, elapsed)
}

// ErrnoName returns a human-readable name for common errno values
// TODO: this should not duplicate the syscall package
func ErrnoName(errno syscall.Errno) string {
	switch errno {
	case 0:
		return "OK"
	case syscall.ENOENT:
		return "ENOENT"
	case syscall.EIO:
		return "EIO"
	case syscall.EEXIST:
		return "EEXIST"
	case syscall.ENOTDIR:
		return "ENOTDIR"
	case syscall.EISDIR:
		return "EISDIR"
	case syscall.ENOTEMPTY:
		return "ENOTEMPTY"
	case syscall.EINVAL:
		return "EINVAL"
	case syscall.EROFS:
		return "EROFS"
	case syscall.EACCES:
		return "EACCES"
	case syscall.EPERM:
		return "EPERM"
	default:
		return fmt.Sprintf("errno(%d)", errno)
	}
}

// KV traces a KV store operation
func (t *Tracer) KV(op string, key interface{}) *TraceKV {
	if !t.IsEnabled() {
		return &TraceKV{tracer: nil}
	}
	return &TraceKV{
		tracer: t,
		op:     op,
		key:    fmt.Sprintf("%v", key),
		start:  time.Now(),
	}
}

// TraceKV represents a traced KV operation
type TraceKV struct {
	tracer *Tracer
	op     string
	key    string
	start  time.Time
}

// Done marks the KV operation as complete
func (k *TraceKV) Done() {
	if k.tracer == nil {
		return
	}
	elapsed := time.Since(k.start)
	k.tracer.logger.Printf("  kv.%s(%s) OK [%v]", k.op, k.key, elapsed)
}

// Error marks the KV operation as failed
func (k *TraceKV) Error(err error) {
	if k.tracer == nil {
		return
	}
	elapsed := time.Since(k.start)
	k.tracer.logger.Printf("  kv.%s(%s) ERROR: %v [%v]", k.op, k.key, err, elapsed)
}
