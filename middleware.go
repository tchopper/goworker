package goworker

import "github.com/kr/pretty"

type queueWorker struct {
	middlewares    []func(WorkerFunc) WorkerFunc
	WorkerFunction WorkerFunc
}

func NewQueueWorker() queueWorker {
	return queueWorker{}
}

func (qw *queueWorker) SetWorkerFunction(worker WorkerFunc) {
	qw.WorkerFunction = worker
}

func (qw *queueWorker) WorkerFunc() WorkerFunc {
	return chain(qw.middlewares, qw.WorkerFunction)
}

func (qw *queueWorker) Use(middlewares func(WorkerFunc) WorkerFunc) {
	pretty.Println(qw.middlewares)
	qw.middlewares = append(qw.middlewares, middlewares)
}

func chain(middlewares []func(WorkerFunc) WorkerFunc, worker WorkerFunc) WorkerFunc {
	// return if the middlewares are blank
	pretty.Println(worker)
	if len(middlewares) == 0 {
		return worker
	}

	// Wrap the end handler with the middleware chain
	h := middlewares[len(middlewares)-1](worker)
	for i := len(middlewares) - 2; i >= 0; i-- {
		pretty.Println(middlewares[i](h))
		h = middlewares[i](h)
	}

	return h
}
