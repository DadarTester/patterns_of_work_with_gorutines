package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

// Структуры данных
type Order struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	Products    []string  `json:"products"`
	TotalAmount float64   `json:"total_amount"`
	Status      string    `json:"status"` // pending, received, confirmed, completed, failed
	CreatedAt   time.Time `json:"created_at"`
	ConfirmedAt time.Time `json:"confirmed_at,omitempty"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

type OrderRequest struct {
	UserID      string   `json:"user_id"`
	Products    []string `json:"products"`
	TotalAmount float64  `json:"total_amount"`
}

// PubSub для событий заказов
type OrderPubSub struct {
	subscribers map[string][]chan Order
	mut         sync.RWMutex
}

func NewOrderPubSub() *OrderPubSub {
	return &OrderPubSub{
		subscribers: make(map[string][]chan Order),
	}
}

func (ps *OrderPubSub) Subscribe(event string) chan Order {
	ps.mut.Lock()
	defer ps.mut.Unlock()

	ch := make(chan Order, 10)
	ps.subscribers[event] = append(ps.subscribers[event], ch)
	return ch
}

func (ps *OrderPubSub) Publish(event string, order Order) {
	ps.mut.RLock()
	defer ps.mut.RUnlock()

	for _, ch := range ps.subscribers[event] {
		select {
		case ch <- order:
		default:
			// Пропускаем если канал полный
		}
	}
}

// Семафоры для ограничения параллельной обработки
type Semaphore struct {
	tokens chan struct{}
}

func NewSemaphore(n int) *Semaphore {
	sem := &Semaphore{
		tokens: make(chan struct{}, n),
	}
	// Заполняем семафор токенами
	for i := 0; i < n; i++ {
		sem.tokens <- struct{}{}
	}
	return sem
}

func (s *Semaphore) Acquire() {
	<-s.tokens
}

func (s *Semaphore) Release() {
	s.tokens <- struct{}{}
}

// Сервисы для каждого этапа
type OrderReceivingService struct {
	semaphore *Semaphore
}

func NewOrderReceivingService() *OrderReceivingService {
	return &OrderReceivingService{
		semaphore: NewSemaphore(5), // Максимум 5 одновременных приемов заказов
	}
}

func (ors *OrderReceivingService) Process(order Order) (Order, error) {
	ors.semaphore.Acquire()
	defer ors.semaphore.Release()

	// Имитация обработки заказа
	time.Sleep(time.Duration(100+rand.Intn(100)) * time.Millisecond)

	// Валидация заказа
	if len(order.Products) == 0 {
		return order, fmt.Errorf("заказ должен содержать товары")
	}
	if order.TotalAmount <= 0 {
		return order, fmt.Errorf("неверная сумма заказа")
	}

	order.Status = "received"
	log.Printf("✅ Заказ %s принят от пользователя %s", order.ID, order.UserID)
	return order, nil
}

type OrderConfirmationService struct {
	semaphore *Semaphore
}

func NewOrderConfirmationService() *OrderConfirmationService {
	return &OrderConfirmationService{
		semaphore: NewSemaphore(3), // Максимум 3 одновременных подтверждения
	}
}

func (ocs *OrderConfirmationService) Process(order Order) (Order, error) {
	ocs.semaphore.Acquire()
	defer ocs.semaphore.Release()

	// Имитация проверки и подтверждения заказа
	time.Sleep(time.Duration(150+rand.Intn(200)) * time.Millisecond)

	// Случайный отказ для демонстрации (10% шанс)
	if rand.Float32() < 0.1 {
		return order, fmt.Errorf("заказ не может быть подтвержден: проблемы с оплатой")
	}

	order.Status = "confirmed"
	order.ConfirmedAt = time.Now()
	log.Printf("✅ Заказ %s подтвержден, сумма: %.2f", order.ID, order.TotalAmount)
	return order, nil
}

type OrderCompletionService struct {
	semaphore *Semaphore
}

func NewOrderCompletionService() *OrderCompletionService {
	return &OrderCompletionService{
		semaphore: NewSemaphore(4), // Максимум 4 одновременных завершения
	}
}

func (ocs *OrderCompletionService) Process(order Order) (Order, error) {
	ocs.semaphore.Acquire()
	defer ocs.semaphore.Release()

	// Имитация финальной обработки заказа
	time.Sleep(time.Duration(80+rand.Intn(120)) * time.Millisecond)

	order.Status = "completed"
	order.CompletedAt = time.Now()
	log.Printf("🎉 Заказ %s завершен! Уведомление отправлено пользователю %s", order.ID, order.UserID)
	return order, nil
}

// Producer-Consumer для обработки заказов
func orderProducer(requests <-chan OrderRequest) <-chan Order {
	orders := make(chan Order, 20)

	go func() {
		for req := range requests {
			order := Order{
				ID:          fmt.Sprintf("order-%d", time.Now().UnixNano()),
				UserID:      req.UserID,
				Products:    req.Products,
				TotalAmount: req.TotalAmount,
				Status:      "pending",
				CreatedAt:   time.Now(),
			}
			orders <- order
		}
		close(orders)
	}()

	return orders
}

// Worker для каждого этапа обработки
func orderProcessingWorker(
	workerID int,
	stage string,
	orders <-chan Order,
	results chan<- Order,
	processor func(Order) (Order, error),
	pubSub *OrderPubSub,
) {
	for order := range orders {
		log.Printf("🎯 Этап '%s' (воркер %d) обрабатывает заказ %s", stage, workerID, order.ID)

		processedOrder, err := processor(order)
		if err != nil {
			processedOrder.Status = "failed"
			log.Printf("❌ Ошибка на этапе '%s' для заказа %s: %v", stage, order.ID, err)
		}

		// Публикуем событие
		event := fmt.Sprintf("order.%s", stage)
		pubSub.Publish(event, processedOrder)

		results <- processedOrder
	}
}

// Fan-in для сбора результатов
func fanInOrderResults(inputs ...<-chan Order) <-chan Order {
	var wg sync.WaitGroup
	out := make(chan Order)

	for _, input := range inputs {
		wg.Add(1)
		go func(ch <-chan Order) {
			defer wg.Done()
			for order := range ch {
				out <- order
			}
		}(input)
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// Pipeline для трех этапов обработки
func createOrderPipeline(
	orders <-chan Order,
	receivingService *OrderReceivingService,
	confirmationService *OrderConfirmationService,
	completionService *OrderCompletionService,
	pubSub *OrderPubSub,
) <-chan Order {

	// Этап 1: Прием заказа (Fan-out с 3 воркерами)
	var receivingWorkers []<-chan Order

	for i := 1; i <= 3; i++ {
		workerOut := make(chan Order)
		go orderProcessingWorker(i, "received", orders, workerOut, receivingService.Process, pubSub)
		receivingWorkers = append(receivingWorkers, workerOut)
	}
	receivingCombined := fanInOrderResults(receivingWorkers...)

	// Этап 2: Подтверждение заказа (Fan-out с 2 воркерами)
	confirmationResults := make(chan Order, 10)
	var confirmationWorkers []<-chan Order

	go func() {
		for order := range receivingCombined {
			if order.Status == "received" {
				confirmationResults <- order
			}
		}
		close(confirmationResults)
	}()

	for i := 1; i <= 2; i++ {
		workerOut := make(chan Order)
		go orderProcessingWorker(i, "confirmed", confirmationResults, workerOut, confirmationService.Process, pubSub)
		confirmationWorkers = append(confirmationWorkers, workerOut)
	}
	confirmationCombined := fanInOrderResults(confirmationWorkers...)

	// Этап 3: Завершение заказа (Fan-out с 3 воркерами)
	completionResults := make(chan Order, 10)
	var completionWorkers []<-chan Order

	go func() {
		for order := range confirmationCombined {
			if order.Status == "confirmed" {
				completionResults <- order
			}
		}
		close(completionResults)
	}()

	for i := 1; i <= 3; i++ {
		workerOut := make(chan Order)
		go orderProcessingWorker(i, "completed", completionResults, workerOut, completionService.Process, pubSub)
		completionWorkers = append(completionWorkers, workerOut)
	}

	return fanInOrderResults(completionWorkers...)
}

// HTTP handlers
type OrderService struct {
	pubSub              *OrderPubSub
	receivingService    *OrderReceivingService
	confirmationService *OrderConfirmationService
	completionService   *OrderCompletionService
	orderRequests       chan OrderRequest
	orderResults        chan Order
}

func NewOrderService() *OrderService {
	os := &OrderService{
		pubSub:              NewOrderPubSub(),
		receivingService:    NewOrderReceivingService(),
		confirmationService: NewOrderConfirmationService(),
		completionService:   NewOrderCompletionService(),
		orderRequests:       make(chan OrderRequest, 50),
		orderResults:        make(chan Order, 50),
	}

	os.startProcessing()
	return os
}

func (os *OrderService) startProcessing() {
	// Producer
	orders := orderProducer(os.orderRequests)

	// Pipeline обработки заказов
	pipelineResults := createOrderPipeline(
		orders,
		os.receivingService,
		os.confirmationService,
		os.completionService,
		os.pubSub,
	)

	// Обработка финальных результатов
	go func() {
		for result := range pipelineResults {
			os.orderResults <- result
			log.Printf("📦 Заказ %s завершил обработку со статусом: %s", result.ID, result.Status)
		}
	}()
}

func (os *OrderService) CreateOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Метод не поддерживается", http.StatusMethodNotAllowed)
		return
	}

	var req OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Неверный JSON", http.StatusBadRequest)
		return
	}

	// Отправляем запрос в канал продюсера
	os.orderRequests <- req

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "accepted",
		"message": "Заказ принят в обработку",
	})
}

func (os *OrderService) EventsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Подписываемся на все события заказов
	events := os.pubSub.Subscribe("order.received")
	confirmedEvents := os.pubSub.Subscribe("order.confirmed")
	completedEvents := os.pubSub.Subscribe("order.completed")

	// Fan-in событий из разных каналов
	allEvents := make(chan Order, 10)

	go func() {
		for event := range events {
			allEvents <- event
		}
	}()

	go func() {
		for event := range confirmedEvents {
			allEvents <- event
		}
	}()

	go func() {
		for event := range completedEvents {
			allEvents <- event
		}
	}()

	for {
		select {
		case order := <-allEvents:
			data, _ := json.Marshal(order)
			fmt.Fprintf(w, "event: %s\n", order.Status)
			fmt.Fprintf(w, "data: %s\n\n", string(data))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}

		case <-r.Context().Done():
			return
		}
	}
}

func (os *OrderService) StatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	stats := map[string]interface{}{
		"service": "Order Processing Service",
		"stages": []map[string]interface{}{
			{
				"name":           "Order Receiving",
				"max_workers":    5,
				"active_workers": 3,
			},
			{
				"name":           "Order Confirmation",
				"max_workers":    3,
				"active_workers": 2,
			},
			{
				"name":           "Order Completion",
				"max_workers":    4,
				"active_workers": 3,
			},
		},
		"timestamp": time.Now(),
	}

	json.NewEncoder(w).Encode(stats)
}

func main() {
	rand.Seed(time.Now().UnixNano())
	service := NewOrderService()

	http.HandleFunc("/order", service.CreateOrderHandler)
	http.HandleFunc("/events", service.EventsHandler)
	http.HandleFunc("/stats", service.StatsHandler)

	// Обслуживаем статические файлы из папки templates
	http.Handle("/", http.FileServer(http.Dir("./templates")))

	log.Println("🚀 Сервис обработки заказов запущен на :8080")
	log.Println("📝 Endpoints:")
	log.Println("   POST /order - создать заказ")
	log.Println("   GET  /events - поток событий SSE")
	log.Println("   GET  /stats - статистика сервиса")
	log.Println("   GET  / - веб-интерфейс")

	log.Fatal(http.ListenAndServe(":8080", nil))
}
