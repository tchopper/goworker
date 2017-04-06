package goworker

import (
	"context"
	"crypto/sha1"
	"fmt"
	"strings"
	"time"

	resque "github.com/everalbum/go-resque"
	"github.com/garyburd/redigo/redis"
	"github.com/kr/pretty"
)

type backoff struct {
	jobName         string
	RetryLimit      int
	BackoffStrategy []int
}

func RetryMiddleware(retryLimit int, jobClass string, backoffStrategy []int) func(next WorkerFunc) WorkerFunc {
	return func(next WorkerFunc) WorkerFunc {
		fn := func(ctx context.Context, queue string, args []interface{}) error {
			retryKey := retryKey(queue, args)

			// Setup the attempt
			retryAttempt, err := beginAttempt(backoffStrategy, retryKey)
			if err != nil {
				return err
			}
			ctx = context.WithValue(ctx, "RedisAttemptNumber", retryAttempt)
			// Run the job
			workerErr := next(ctx, queue, args)

			// Get redis connection
			conn, err := GetConn()
			if err != nil {
				return err
			}
			defer PutConn(conn)

			// Success, just clear the retry key
			if workerErr == nil {
				conn.Do("DEL", retryKey)
				return nil
			}

			if retryAttempt >= retryLimit {
				// If we've retried too many times, give up
				conn.Do("DEL", retryKey)
			} else {
				// Otherwise schedule the retry attempt
				seconds := retryDelay(backoffStrategy, retryAttempt)

				if seconds <= 0 {
					// If there's no delay, just enqueue it
					_, err = resque.Enqueue(conn.Conn, queue, jobClass, args[0])
				} else {
					// Otherwise schedule it
					delay := time.Duration(seconds) * time.Second
					err = resque.EnqueueIn(conn.Conn, delay, queue, jobClass, args[0])
				}

				if err != nil {
					return err
				}
			}

			// Wrap the error
			//
			return nil
		}
		return WorkerFunc(fn)
	}
}

func NewBackoff(jobName string) *backoff {
	eb := new(backoff)
	eb.jobName = jobName

	// Default backoff strategy in seconds
	eb.BackoffStrategy = []int{0, 60, 600, 3600, 10800, 21600} // 0s, 1m, 10m, 1h, 3h, 6h
	eb.RetryLimit = len(eb.BackoffStrategy)
	return eb
}

func beginAttempt(backoffStrategy []int, retryKey string) (int, error) {
	conn, err := GetConn()
	if err != nil {
		return -1, err
	}
	defer PutConn(conn)

	// Create the retry key if not exists
	_, err = conn.Do("SETNX", retryKey, -1)
	if err != nil {
		return -1, err
	}

	// Increment the attempt we're on
	retryAttempt, err := redis.Int(conn.Do("INCR", retryKey))
	if err != nil {
		return -1, err
	}

	// Expire the retry key so we don't leave it hanging
	// (an hour after it was supposed to be removed)
	conn.Do("EXPIRE", retryKey, retryDelay(backoffStrategy, retryAttempt)+3600)

	return retryAttempt, nil
}

func retryDelay(BackoffStrategy []int, attempt int) int {
	if attempt > (len(BackoffStrategy) - 1) {
		attempt = len(BackoffStrategy) - 1
	}
	return BackoffStrategy[attempt]
}

func retryKey(jobName string, args []interface{}) string {
	parts := []string{"resque", "resque-retry", jobName, retryIdentifier(args)}
	pretty.Println(strings.Join(parts, ":"))
	return strings.Join(parts, ":")
}

func retryIdentifier(args []interface{}) string {
	params := make([]string, len(args))
	for i, value := range args {
		params[i] = fmt.Sprintf("%v", value)
	}

	h := sha1.New()
	h.Write([]byte(strings.Join(params, "-")))
	bs := h.Sum(nil)

	hash := fmt.Sprintf("%x", bs)

	return strings.Replace(hash, " ", "", -1)
}
