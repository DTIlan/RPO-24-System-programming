package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"lab/concurrency/concurrency"
)

type Result = concurrency.Result

func runPool(ctx context.Context, inputs []int, workers int) ([]Result, error) {
	return concurrency.RunPool(ctx, inputs, workers)
}

func printResults(results []Result) {
	for _, result := range results {
		fmt.Printf("%d -> %d\n", result.Input, result.Square)
	}
}

type experiment func() (time.Duration, []Result, error)

func runThreeTimes(name string, run experiment) {
	times := make([]time.Duration, 0, 3)
	var lastResults []Result
	for i := 0; i < 3; i++ {
		elapsed, results, err := run()
		if err != nil {
			fmt.Printf("%s, запуск %d: ошибка: %v\n", name, i+1, err)
		}
		times = append(times, elapsed)
		lastResults = results
	}

	sorted := append([]time.Duration(nil), times...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	fmt.Printf("%s: медиана %v, разброс %v–%v\n", name, sorted[1], sorted[0], sorted[2])
	fmt.Printf("Результатов в последнем запуске: %d\n", len(lastResults))
	printResults(lastResults)
	fmt.Println()
}

func main() {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8}

	runThreeTimes("Последовательная обработка", func() (time.Duration, []Result, error) {
		results, elapsed, err := concurrency.RunSequential(context.Background(), inputs)
		return elapsed, results, err
	})

	runThreeTimes("Пул из 1 обработчика", func() (time.Duration, []Result, error) {
		start := time.Now()
		results, err := runPool(context.Background(), inputs, 1)
		return time.Since(start), results, err
	})

	runThreeTimes("Пул из 4 обработчиков", func() (time.Duration, []Result, error) {
		start := time.Now()
		results, err := runPool(context.Background(), inputs, 4)
		return time.Since(start), results, err
	})

	fmt.Println("Пул из 4 обработчиков, таймаут 300 мс:")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	partial, timeoutErr := runPool(ctx, inputs, 4)
	cancel()
	fmt.Printf("Получено результатов: %d из %d\n", len(partial), len(inputs))
	printResults(partial)
	if errors.Is(timeoutErr, context.DeadlineExceeded) {
		fmt.Println("Ожидаемая ошибка:", timeoutErr)
	} else {
		fmt.Println("Ошибка:", timeoutErr)
	}
}
