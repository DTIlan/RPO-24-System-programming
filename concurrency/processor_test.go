package concurrency

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

var testInputs = []int{1, 2, 3, 4, 5, 6, 7, 8}

func TestRunSequential(t *testing.T) {
	results, elapsed, err := RunSequential(context.Background(), testInputs)
	if err != nil {
		t.Fatalf("RunSequential вернул ошибку: %v", err)
	}
	if err := ValidateResults(testInputs, results); err != nil {
		t.Fatal(err)
	}
	if elapsed < 8*TaskDelay {
		t.Fatalf("последовательная обработка завершилась неожиданно быстро: %v", elapsed)
	}
}

func TestRunPoolOneWorker(t *testing.T) {
	results, err := RunPool(context.Background(), testInputs, 1)
	if err != nil {
		t.Fatalf("RunPool вернул ошибку: %v", err)
	}
	if err := ValidateResults(testInputs, results); err != nil {
		t.Fatal(err)
	}
}

func TestRunPoolSeveralWorkers(t *testing.T) {
	results, err := RunPool(context.Background(), testInputs, 4)
	if err != nil {
		t.Fatalf("RunPool вернул ошибку: %v", err)
	}
	if err := ValidateResults(testInputs, results); err != nil {
		t.Fatal(err)
	}
	want := []Result{{1, 1}, {2, 4}, {3, 9}, {4, 16}, {5, 25}, {6, 36}, {7, 49}, {8, 64}}
	if !reflect.DeepEqual(results, want) {
		t.Fatalf("неверные результаты: got %v, want %v", results, want)
	}
}

func TestRunPoolInvalidWorkers(t *testing.T) {
	_, err := RunPool(context.Background(), testInputs, 0)
	if err == nil {
		t.Fatal("ожидалась ошибка при workers = 0")
	}
}

func TestRunPoolTimeoutReturnsPartialResults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	results, err := RunPool(ctx, testInputs, 4)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ожидалась context.DeadlineExceeded, получено: %v", err)
	}
	if len(results) == 0 || len(results) >= len(testInputs) {
		t.Fatalf("ожидалось частичное количество результатов, получено %d", len(results))
	}
	for _, result := range results {
		if result.Square != result.Input*result.Input {
			t.Errorf("неверный квадрат для %d: %d", result.Input, result.Square)
		}
	}
}

func TestRunPoolAlreadyCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	results, err := RunPool(ctx, testInputs, 4)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ожидалась context.Canceled, получено: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("не ожидалось результатов для заранее отменённого контекста: %v", results)
	}
}

func TestRunPoolEmptyInput(t *testing.T) {
	results, err := RunPool(context.Background(), nil, 2)
	if err != nil {
		t.Fatalf("не ожидалась ошибка: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("ожидался пустой результат, получено %v", results)
	}
}
