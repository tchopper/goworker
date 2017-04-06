package goworker

import "context"

type WorkerFunc func(context.Context, string, []interface{}) error
