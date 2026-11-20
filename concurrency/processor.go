package concurrency

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

const TaskDelay = 200 * time.Millisecond

type Result struct {
	Input  int
	Square int
}

func ProcessOne(ctx context.Context, input int) (Result, error) {
	timer := time.NewTimer(TaskDelay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return Result{Input: input, Square: input * input}, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func RunSequential(ctx context.Context, inputs []int) ([]Result, time.Duration, error) {
	started := time.Now()
	results := make([]Result, 0, len(inputs))

	for _, input := range inputs {
		result, err := ProcessOne(ctx, input)
		if err != nil {
			return results, time.Since(started), err
		}
		results = append(results, result)
	}

	return results, time.Since(started), nil
}

func RunPool(ctx context.Context, inputs []int, workers int) ([]Result, error) {
	if workers < 1 {
		return nil, errors.New("количество обработчиков должно быть не меньше 1")
	}
	if err := ctx.Err(); err != nil {
		return []Result{}, err
	}
	if len(inputs) == 0 {
		return []Result{}, nil
	}

	jobs := make(chan int)
	results := make(chan Result, len(inputs))
	senderDone := make(chan struct{})

	go func() {
		defer close(senderDone)
		defer close(jobs)
		for _, input := range inputs {
			select {
			case jobs <- input:
			case <-ctx.Done():
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case input, ok := <-jobs:
					if !ok {
						return
					}

					result, err := ProcessOne(ctx, input)
					if err != nil {
						return
					}

					select {
					case results <- result:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	collected := make([]Result, 0, len(inputs))
	for result := range results {
		collected = append(collected, result)
	}

	<-senderDone

	sort.Slice(collected, func(i, j int) bool {
		return collected[i].Input < collected[j].Input
	})

	if err := ctx.Err(); err != nil {
		return collected, err
	}
	return collected, nil
}

func ValidateResults(inputs []int, results []Result) error {
	if len(inputs) != len(results) {
		return fmt.Errorf("получено %d результатов вместо %d", len(results), len(inputs))
	}
	for i, input := range inputs {
		if results[i].Input != input {
			return fmt.Errorf("результат %d: ожидалось число %d, получено %d", i, input, results[i].Input)
		}
		if results[i].Square != input*input {
			return fmt.Errorf("для числа %d ожидался квадрат %d, получено %d", input, input*input, results[i].Square)
		}
	}
	return nil
}
