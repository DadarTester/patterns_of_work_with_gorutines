package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Semaphore struct {
	sem chan struct{}
}

type Price struct {
	original    float64
	discount    float64
	priceeighty float64
	final       float64
	err         error
}

func producer(ch chan<- int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < 10; i++ {
		ch <- i
	}
	close(ch)
}

func consumer(ch <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for num := range ch {
		fmt.Println(num)
	}
}

func NewSem(n int) *Semaphore {
	return &Semaphore{
		sem: make(chan struct{}, n),
	}
}

func (s *Semaphore) Aсquire() {
	s.sem <- struct{}{}
}

func (s *Semaphore) Release() {
	<-s.sem
}

func worker(id int, sem *Semaphore, wg *sync.WaitGroup) {
	defer wg.Done()

	sem.Aсquire()
	defer sem.Release()

	fmt.Println(id)
	time.Sleep(time.Second)
	fmt.Println(id)
}

func priceGenerator(ctx context.Context, price ...float64) <-chan float64 {
	out := make(chan float64)
	go func() {
		defer close(out)
		for _, prices := range price {
			select {
			case <-ctx.Done():
				return
			case out <- prices:
				time.Sleep(80 * time.Millisecond)
			}
		}
	}()
	return out
}

func calcDiscountPipeline(ctx context.Context, prices <-chan float64, numWorkers int) <-chan Price {
	eightyPercentStage := calcEightyPercent(ctx, prices, numWorkers)
	return calculateFinalPrice(ctx, eightyPercentStage, numWorkers)
}
func calcEightyPercent(ctx context.Context, price <-chan float64, numWorkers int) <-chan Price {
	workerChannels := make([]<-chan Price, numWorkers)
	for i := 0; i < numWorkers; i++ {
		workerChannels[i] = eightyPercentWorker(ctx, price, i)
	}
	return mergePriceResults(ctx, workerChannels...)
}
func eightyPercentWorker(ctx context.Context, prices <-chan float64, workerID int) <-chan Price {
	out := make(chan Price)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case price, ok := <-prices:
				if !ok {
					return
				}
				result := Price{
					original:    price,
					priceeighty: price * 0.8,
				}
				fmt.Println(workerID, price, result.priceeighty)
				time.Sleep(100 * time.Millisecond)

				select {
				case <-ctx.Done():
					return
				case out <- result:
				}
			}
		}
	}()
	return out
}
func calculateFinalPrice(ctx context.Context, input <-chan Price, numWorkers int) <-chan Price {
	workerChannels := make([]<-chan Price, numWorkers)

	for i := 0; i < numWorkers; i++ {
		workerChannels[i] = finalPriceWorker(ctx, input, i)
	}

	return mergePriceResults(ctx, workerChannels...)
}

// Worker для расчета финальной цены
func finalPriceWorker(ctx context.Context, input <-chan Price, workerID int) <-chan Price {
	out := make(chan Price)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case data, ok := <-input:
				if !ok {
					return
				}

				if data.err != nil {
					out <- data
					continue
				}
				data.discount = data.original - data.priceeighty
				data.final = data.priceeighty
				fmt.Println(workerID, data.original, data.discount, data.final)
				time.Sleep(100 * time.Millisecond)
				select {
				case <-ctx.Done():
					return
				case out <- data:
				}
			}
		}
	}()
	return out
}
func mergePriceResults(ctx context.Context, channels ...<-chan Price) <-chan Price {
	var wg sync.WaitGroup
	out := make(chan Price)

	output := func(ch <-chan Price) {
		defer wg.Done()
		for data := range ch {
			select {
			case <-ctx.Done():
				return
			case out <- data:
			}
		}
	}

	wg.Add(len(channels))
	for _, ch := range channels {
		go output(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

func main() {
	ch := make(chan int)
	var wg sync.WaitGroup
	wg.Add(1)
	go producer(ch, &wg)

	wg.Add(1)
	go consumer(ch, &wg)
	wg.Wait()

	const (
		totalWorkers  = 10
		maxConcurrent = 5
	)
	sem := NewSem(maxConcurrent)
	for i := 1; i <= totalWorkers; i++ {
		wg.Add(1)
		go worker(i, sem, &wg)
	}
	ctx := context.Background()

	prices := priceGenerator(ctx, 100.0, 3167.4317, 275.0, 750.0, 1488.0)
	result := calcDiscountPipeline(ctx, prices, 5)
	fmt.Println("Расчет скидки в 20%")
	fmt.Println("Итого")
	for res := range result {
		if res.err != nil {
			fmt.Println(res.err)
			continue
		}
		fmt.Println(res.original, res.discount, res.priceeighty, res.final)
		wg.Wait()
	}
}
