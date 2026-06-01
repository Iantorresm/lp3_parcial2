#include "delivery_system.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static void exit_with_pthread_error(const char *operation, int error_code)
{
    fprintf(stderr, "Error en %s: %s\n", operation, strerror(error_code));
    exit(EXIT_FAILURE);
}

static void lock_mutex(pthread_mutex_t *mutex, const char *mutex_name)
{
    int result = pthread_mutex_lock(mutex);

    if (result != 0) {
        exit_with_pthread_error(mutex_name, result);
    }
}

static void unlock_mutex(pthread_mutex_t *mutex, const char *mutex_name)
{
    int result = pthread_mutex_unlock(mutex);

    if (result != 0) {
        exit_with_pthread_error(mutex_name, result);
    }
}

static void wait_for_semaphore(sem_t *semaphore, const char *semaphore_name)
{
    while (sem_wait(semaphore) == -1) {
        if (errno == EINTR) {
            continue;
        }

        perror(semaphore_name);
        exit(EXIT_FAILURE);
    }
}

static void post_to_semaphore(sem_t *semaphore, const char *semaphore_name)
{
    if (sem_post(semaphore) == -1) {
        perror(semaphore_name);
        exit(EXIT_FAILURE);
    }
}

static int reserve_next_order_id(DeliverySystem *system)
{
    int order_id;

    lock_mutex(&system->order_id_mutex, "bloquear mutex de IDs");
    order_id = system->next_order_id;
    system->next_order_id++;
    unlock_mutex(&system->order_id_mutex, "desbloquear mutex de IDs");

    return order_id;
}

static void register_generated_order(DeliverySystem *system)
{
    lock_mutex(&system->counters_mutex, "bloquear mutex de contadores");
    system->generated_orders++;
    unlock_mutex(&system->counters_mutex, "desbloquear mutex de contadores");
}

static void register_prepared_order(DeliverySystem *system)
{
    lock_mutex(&system->counters_mutex, "bloquear mutex de contadores");
    system->prepared_orders++;
    unlock_mutex(&system->counters_mutex, "desbloquear mutex de contadores");
}

static void register_delivered_order(DeliverySystem *system)
{
    lock_mutex(&system->counters_mutex, "bloquear mutex de contadores");
    system->delivered_orders++;
    unlock_mutex(&system->counters_mutex, "desbloquear mutex de contadores");
}

static void print_with_mutex(DeliverySystem *system, const char *message)
{
    lock_mutex(&system->print_mutex, "bloquear mutex de impresión");
    printf("%s\n", message);
    fflush(stdout);
    unlock_mutex(&system->print_mutex, "desbloquear mutex de impresión");
}

static void log_order_generated(DeliverySystem *system, int producer_id, const Order *order)
{
    char message[256];

    snprintf(
        message,
        sizeof(message),
        "[Productor %d] Pedido generado: id=%d, comida=%s, preparación=%u segundos",
        producer_id,
        order->id,
        food_type_to_text(order->food_type),
        order->preparation_seconds);

    print_with_mutex(system, message);
}

static void log_order_taken_by_cook(DeliverySystem *system, int cook_id, const Order *order)
{
    char message[256];

    snprintf(
        message,
        sizeof(message),
        "[Cocinero %d] Pedido tomado: id=%d, comida=%s",
        cook_id,
        order->id,
        food_type_to_text(order->food_type));

    print_with_mutex(system, message);
}

static void log_order_delivered(DeliverySystem *system, int delivery_person_id, const Order *order)
{
    char message[256];

    snprintf(
        message,
        sizeof(message),
        "[Repartidor %d] Pedido entregado: id=%d, comida=%s",
        delivery_person_id,
        order->id,
        food_type_to_text(order->food_type));

    print_with_mutex(system, message);
}

int delivery_system_init(DeliverySystem *system)
{
    int result;

    system->next_order_id = 1;
    system->generated_orders = 0;
    system->prepared_orders = 0;
    system->delivered_orders = 0;

    result = order_queue_init(&system->pending_orders, "pedidos pendientes");
    if (result != 0) {
        return result;
    }

    result = order_queue_init(&system->ready_orders, "pedidos listos para entrega");
    if (result != 0) {
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    result = pthread_mutex_init(&system->order_id_mutex, NULL);
    if (result != 0) {
        order_queue_destroy(&system->ready_orders);
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    result = pthread_mutex_init(&system->counters_mutex, NULL);
    if (result != 0) {
        pthread_mutex_destroy(&system->order_id_mutex);
        order_queue_destroy(&system->ready_orders);
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    result = pthread_mutex_init(&system->print_mutex, NULL);
    if (result != 0) {
        pthread_mutex_destroy(&system->counters_mutex);
        pthread_mutex_destroy(&system->order_id_mutex);
        order_queue_destroy(&system->ready_orders);
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    return 0;
}

void delivery_system_destroy(DeliverySystem *system)
{
    pthread_mutex_destroy(&system->print_mutex);
    pthread_mutex_destroy(&system->counters_mutex);
    pthread_mutex_destroy(&system->order_id_mutex);
    order_queue_destroy(&system->ready_orders);
    order_queue_destroy(&system->pending_orders);
}

int order_queue_init(OrderQueue *queue, const char *name)
{
    int result;

    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->name = name;

    result = pthread_mutex_init(&queue->mutex, NULL);
    if (result != 0) {
        return result;
    }

    if (sem_init(&queue->available_slots, 0, QUEUE_CAPACITY) == -1) {
        result = errno;
        pthread_mutex_destroy(&queue->mutex);
        return result;
    }

    if (sem_init(&queue->available_items, 0, 0) == -1) {
        result = errno;
        sem_destroy(&queue->available_slots);
        pthread_mutex_destroy(&queue->mutex);
        return result;
    }

    return 0;
}

void order_queue_destroy(OrderQueue *queue)
{
    sem_destroy(&queue->available_items);
    sem_destroy(&queue->available_slots);
    pthread_mutex_destroy(&queue->mutex);
}

void order_queue_push(OrderQueue *queue, Order order)
{
    wait_for_semaphore(&queue->available_slots, "esperar espacio libre en cola");

    lock_mutex(&queue->mutex, "bloquear mutex de cola");
    queue->items[queue->tail] = order;
    queue->tail = (queue->tail + 1) % QUEUE_CAPACITY;
    queue->count++;
    unlock_mutex(&queue->mutex, "desbloquear mutex de cola");

    post_to_semaphore(&queue->available_items, "avisar item disponible en cola");
}

Order order_queue_pop(OrderQueue *queue)
{
    Order order;

    wait_for_semaphore(&queue->available_items, "esperar item disponible en cola");

    lock_mutex(&queue->mutex, "bloquear mutex de cola");
    order = queue->items[queue->head];
    queue->head = (queue->head + 1) % QUEUE_CAPACITY;
    queue->count--;
    unlock_mutex(&queue->mutex, "desbloquear mutex de cola");

    post_to_semaphore(&queue->available_slots, "avisar espacio libre en cola");

    return order;
}

Order create_order(int order_id, int producer_id)
{
    Order order;

    order.id = order_id;
    order.producer_id = producer_id;
    order.food_type = (FoodType)((order_id - 1) % FOOD_TYPE_COUNT);
    order.preparation_seconds = calculate_preparation_time(order_id);
    order.kind = ORDER_KIND_REAL;

    return order;
}

Order create_stop_order(void)
{
    Order order;

    order.id = -1;
    order.producer_id = -1;
    order.food_type = FOOD_TYPE_PIZZA;
    order.preparation_seconds = 0;
    order.kind = ORDER_KIND_STOP;

    return order;
}

const char *food_type_to_text(FoodType food_type)
{
    switch (food_type) {
    case FOOD_TYPE_PIZZA:
        return "Pizza";
    case FOOD_TYPE_HAMBURGER:
        return "Hamburguesa";
    case FOOD_TYPE_PASTA:
        return "Pasta";
    case FOOD_TYPE_SALAD:
        return "Ensalada";
    default:
        return "Comida desconocida";
    }
}

unsigned int calculate_preparation_time(int order_id)
{
    const unsigned int preparation_range =
        MAX_PREPARATION_SECONDS - MIN_PREPARATION_SECONDS + 1;

    return MIN_PREPARATION_SECONDS + (unsigned int)((order_id - 1) % preparation_range);
}

void enqueue_stop_orders(OrderQueue *queue, int stop_order_count)
{
    int index;

    /*
     * Los pedidos centinela son la señal de cierre de una etapa.
     * No representan comida real: sirven para despertar consumidores bloqueados
     * y permitir que cada thread termine de forma ordenada.
     */
    for (index = 0; index < stop_order_count; index++) {
        order_queue_push(queue, create_stop_order());
    }
}

void print_final_summary(DeliverySystem *system)
{
    int generated_orders;
    int prepared_orders;
    int delivered_orders;

    lock_mutex(&system->counters_mutex, "bloquear mutex de contadores");
    generated_orders = system->generated_orders;
    prepared_orders = system->prepared_orders;
    delivered_orders = system->delivered_orders;
    unlock_mutex(&system->counters_mutex, "desbloquear mutex de contadores");

    lock_mutex(&system->print_mutex, "bloquear mutex de impresión");
    printf("\nResumen final\n");
    printf("-------------\n");
    printf("Pedidos generados:  %d\n", generated_orders);
    printf("Pedidos preparados: %d\n", prepared_orders);
    printf("Pedidos entregados: %d\n", delivered_orders);
    unlock_mutex(&system->print_mutex, "desbloquear mutex de impresión");
}

void *producer_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;
    DeliverySystem *system = context->system;
    int order_index;

    for (order_index = 0; order_index < ORDERS_PER_PRODUCER; order_index++) {
        const int order_id = reserve_next_order_id(system);
        const Order order = create_order(order_id, context->worker_id);

        log_order_generated(system, context->worker_id, &order);
        order_queue_push(&system->pending_orders, order);
        register_generated_order(system);

        sleep(GENERATION_PAUSE_SECONDS);
    }

    return NULL;
}

void *cook_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;
    DeliverySystem *system = context->system;

    while (1) {
        Order order = order_queue_pop(&system->pending_orders);

        if (order.kind == ORDER_KIND_STOP) {
            return NULL;
        }

        log_order_taken_by_cook(system, context->worker_id, &order);
        sleep(order.preparation_seconds);

        order_queue_push(&system->ready_orders, order);
        register_prepared_order(system);
    }
}

void *delivery_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;
    DeliverySystem *system = context->system;

    while (1) {
        Order order = order_queue_pop(&system->ready_orders);

        if (order.kind == ORDER_KIND_STOP) {
            return NULL;
        }

        sleep(DELIVERY_SECONDS);
        register_delivered_order(system);
        log_order_delivered(system, context->worker_id, &order);
    }
}
