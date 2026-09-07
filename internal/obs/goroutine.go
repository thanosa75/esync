package obs

import (
	"fmt"
	"runtime/debug"
	"sync"

	"esync/internal/fault"
)

// obs is the one package permitted a bare `go` statement (ARCHITECTURE §14.5);
// everywhere else goroutines start via Go.

var (
	fatalMu   sync.RWMutex
	fatalFn   func(error)
	pkgRootMu sync.RWMutex
	pkgRoot   Ctx
	pkgRootOK bool
)

// OnFatal registers the process-wide handler invoked on a Fatal fault (a
// recovered panic, or a Fatal-class error returned by an obs.Go function). Pass
// nil to clear it. The handler typically emits the summary and exits.
func OnFatal(h func(error)) {
	fatalMu.Lock()
	fatalFn = h
	fatalMu.Unlock()
}

func setPkgRoot(ctx Ctx) {
	pkgRootMu.Lock()
	pkgRoot = ctx
	pkgRootOK = true
	pkgRootMu.Unlock()
}

func triggerFatal(err error) {
	pkgRootMu.RLock()
	root, ok := pkgRoot, pkgRootOK
	pkgRootMu.RUnlock()
	if ok {
		DumpRing(root)
	}
	fatalMu.RLock()
	h := fatalFn
	fatalMu.RUnlock()
	if h != nil {
		h(err)
	}
}

// Go is the only sanctioned goroutine launcher (ARCHITECTURE §14.5). It installs
// a recover that converts a panic to an E9001 fault (with the stack trace and the
// goroutine's trace fields) and routes it to the OnFatal handler. A non-nil error
// returned by fn is routed to OnFatal if it is Fatal-class, otherwise logged.
func Go(ctx Ctx, name string, fn func() error) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				f := fault.Newf(fault.E9001, "goroutine", name, nil, "panic: %v", r).
					WithCtx(fault.Fields{"goroutine": name, "stack": stack})
				for k, v := range ctx.tf {
					f.Ctx[k] = v
				}
				Error(ctx, "goroutine panic",
					F("code", string(fault.E9001)), F("name", name), F("err", fmt.Sprint(r)))
				triggerFatal(f)
			}
		}()
		if err := fn(); err != nil {
			if fault.IsFatal(err) {
				LogFault(ctx, err)
				triggerFatal(err)
			} else {
				LogFault(ctx, err)
			}
		}
	}()
}
