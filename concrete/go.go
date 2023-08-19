package concrete

import (
	"context"
	"powerlaw.ai/powerlib/migrate"
)

// goExecutor 用于存储 go 处理单元
type goExecutor struct {
	handlers []GoHandler
}

func NewGoExecutor(handlers ...GoHandler) migrate.Executor {
	return &goExecutor{
		handlers: handlers,
	}
}

func (g *goExecutor) ListHandlers() ([]migrate.Handler, error) {
	var handlers []migrate.Handler
	for idx := range g.handlers {
		handlers = append(handlers, &g.handlers[idx])
	}
	return handlers, nil
}

// GoHandler 存储具体 go 处理程序
type GoHandler struct {
	baseHandler
	executor GoFunc
}

type GoFunc func(ctx context.Context) error

func (g *GoHandler) Exec(ctx context.Context) error {
	return g.executor(ctx)
}

func NewGoHandler(index int, f GoFunc) GoHandler {
	return GoHandler{
		baseHandler: baseHandler{index},
		executor:    f,
	}
}
