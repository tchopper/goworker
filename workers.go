package goworker

import (
	"encoding/json"
	"fmt"
)

var (
	workers map[string]WorkerFunc
)

func init() {
	workers = make(map[string]WorkerFunc)
}

// Register registers a goworker worker function. Class
// refers to the Ruby name of the class which enqueues the
// job. Worker is a function which accepts a queue and an
// arbitrary array of interfaces as arguments.
func Register(class string, worker WorkerFunc) {
	workers[class] = worker
}

func Enqueue(job *Job) error {
	err := Init()
	if err != nil {
		return err
	}

	conn, err := GetConn()
	if err != nil {
		logger.Criticalf("Error on getting connection on enqueue")
		return err
	}
	defer PutConn(conn)

	buffer, err := json.Marshal(job.Payload)
	if err != nil {
		logger.Criticalf("Cant marshal payload on enqueue")
		return err
	}
	err = conn.Send("RPUSH", fmt.Sprintf("%squeue:%s", workerSettings.Namespace, job.Queue), buffer)
	if err != nil {
		logger.Criticalf("Cant push to queue")
		return err
	}

	return conn.Flush()
}
