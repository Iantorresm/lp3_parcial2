/*
 * Simulacro 1: pedidos prioritarios.
 *
 * Extensión evaluada:
 * algunos pedidos son URGENTES y los cocineros deben tomarlos antes que los
 * pedidos normales, sin busy waiting y respetando capacidad máxima de cola.
 */

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PRODUCER_COUNT 3
#define COOK_COUNT 2
#define DELIVERY_PERSON_COUNT 2
#define QUEUE_CAPACITY 5
#define ORDERS_PER_PRODUCER 4

#define MIN_PREPARATION_SECONDS 1
#define MAX_PREPARATION_SECONDS 3
#define DELIVERY_SECONDS 1
#define GENERATION_PAUSE_SECONDS 1

typedef enum {
    FOOD_TYPE_PIZZA = 0,
    FOOD_TYPE_HAMBURGER,
    FOOD_TYPE_PASTA,
    FOOD_TYPE_SALAD,
    FOOD_TYPE_COUNT
} FoodType;

typedef enum {
    ORDER_PRIORITY_NORMAL = 0,
    ORDER_PRIORITY_URGENT
} OrderPriority;

typedef enum {
    ORDER_KIND_REAL = 0,
    ORDER_KIND_STOP
} OrderKind;

typedef struct {
    int id;
    int producer_id;
    FoodType food_type;
    OrderPriority priority;
    unsigned int preparation_seconds;
    OrderKind kind;
} Order;

typedef struct {
    Order items[QUEUE_CAPACITY];
    int head;
    int tail;
    int count;
    int prefer_urgent_orders;
    const char *name;
    pthread_mutex_t mutex;
    sem_t available_slots;
    sem_t available_items;
} OrderQueue;

typedef struct {
    OrderQueue pending_orders;
    OrderQueue ready_orders;
    int next_order_id;
    int generated_orders;
    int prepared_orders;
    int delivered_orders;
    pthread_mutex_t order_id_mutex;
    pthread_mutex_t counters_mutex;
    pthread_mutex_t print_mutex;
} DeliverySystem;

typedef struct {
    int worker_id;
    DeliverySystem *system;
} WorkerContext;

static void finish_with_error(const char *operation, int error_code)
{
    fprintf(stderr, "Error en %s: %s\n", operation, strerror(error_code));
    exit(EXIT_FAILURE);
}

static void lock_mutex(pthread_mutex_t *mutex, const char *operation)
{
    int result = pthread_mutex_lock(mutex);
    if (result != 0) {
        finish_with_error(operation, result);
    }
}

static void unlock_mutex(pthread_mutex_t *mutex, const char *operation)
{
    int result = pthread_mutex_unlock(mutex);
    if (result != 0) {
        finish_with_error(operation, result);
    }
}

static void wait_for_semaphore(sem_t *semaphore, const char *operation)
{
    while (sem_wait(semaphore) == -1) {
        if (errno == EINTR) {
            continue;
        }
        perror(operation);
        exit(EXIT_FAILURE);
    }
}

static void post_to_semaphore(sem_t *semaphore, const char *operation)
{
    if (sem_post(semaphore) == -1) {
        perror(operation);
        exit(EXIT_FAILURE);
    }
}

static int order_queue_init(OrderQueue *queue, const char *name, int prefer_urgent_orders)
{
    int result;

    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->prefer_urgent_orders = prefer_urgent_orders;
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

static void order_queue_destroy(OrderQueue *queue)
{
    sem_destroy(&queue->available_items);
    sem_destroy(&queue->available_slots);
    pthread_mutex_destroy(&queue->mutex);
}

static void order_queue_push(OrderQueue *queue, Order order)
{
    wait_for_semaphore(&queue->available_slots, "esperar espacio disponible");
    lock_mutex(&queue->mutex, "bloquear cola");

    queue->items[queue->tail] = order;
    queue->tail = (queue->tail + 1) % QUEUE_CAPACITY;
    queue->count++;

    unlock_mutex(&queue->mutex, "desbloquear cola");
    post_to_semaphore(&queue->available_items, "avisar pedido disponible");
}

static int select_order_position(OrderQueue *queue)
{
    int position;

    if (!queue->prefer_urgent_orders) {
        return 0;
    }

    /*
     * La cola sigue teniendo capacidad máxima única. La prioridad se resuelve
     * al retirar: se busca el primer pedido urgente respetando FIFO entre
     * urgentes. Si no hay urgentes, sale el más antiguo.
     */
    for (position = 0; position < queue->count; position++) {
        int physical_index = (queue->head + position) % QUEUE_CAPACITY;
        Order candidate = queue->items[physical_index];

        if (candidate.kind == ORDER_KIND_REAL &&
            candidate.priority == ORDER_PRIORITY_URGENT) {
            return position;
        }
    }

    return 0;
}

static Order remove_order_at_position(OrderQueue *queue, int selected_position)
{
    int position;
    int selected_index = (queue->head + selected_position) % QUEUE_CAPACITY;
    Order selected_order = queue->items[selected_index];

    for (position = selected_position; position > 0; position--) {
        int current_index = (queue->head + position) % QUEUE_CAPACITY;
        int previous_index = (queue->head + position - 1) % QUEUE_CAPACITY;
        queue->items[current_index] = queue->items[previous_index];
    }

    queue->head = (queue->head + 1) % QUEUE_CAPACITY;
    queue->count--;

    return selected_order;
}

static Order order_queue_pop(OrderQueue *queue)
{
    int selected_position;
    Order order;

    wait_for_semaphore(&queue->available_items, "esperar pedido disponible");
    lock_mutex(&queue->mutex, "bloquear cola");

    selected_position = select_order_position(queue);
    order = remove_order_at_position(queue, selected_position);

    unlock_mutex(&queue->mutex, "desbloquear cola");
    post_to_semaphore(&queue->available_slots, "avisar espacio disponible");

    return order;
}

static int delivery_system_init(DeliverySystem *system)
{
    int result;

    system->next_order_id = 1;
    system->generated_orders = 0;
    system->prepared_orders = 0;
    system->delivered_orders = 0;

    result = order_queue_init(&system->pending_orders, "pendientes", 1);
    if (result != 0) {
        return result;
    }

    result = order_queue_init(&system->ready_orders, "listos", 0);
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

static void delivery_system_destroy(DeliverySystem *system)
{
    pthread_mutex_destroy(&system->print_mutex);
    pthread_mutex_destroy(&system->counters_mutex);
    pthread_mutex_destroy(&system->order_id_mutex);
    order_queue_destroy(&system->ready_orders);
    order_queue_destroy(&system->pending_orders);
}

static const char *food_type_to_text(FoodType food_type)
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
        return "Desconocido";
    }
}

static const char *priority_to_text(OrderPriority priority)
{
    return priority == ORDER_PRIORITY_URGENT ? "URGENTE" : "NORMAL";
}

static unsigned int calculate_preparation_time(int order_id)
{
    return MIN_PREPARATION_SECONDS +
           (unsigned int)((order_id - 1) %
                          (MAX_PREPARATION_SECONDS - MIN_PREPARATION_SECONDS + 1));
}

static Order create_order(int order_id, int producer_id)
{
    Order order;

    order.id = order_id;
    order.producer_id = producer_id;
    order.food_type = (FoodType)((order_id - 1) % FOOD_TYPE_COUNT);
    order.priority = (order_id % 3 == 0) ? ORDER_PRIORITY_URGENT : ORDER_PRIORITY_NORMAL;
    order.preparation_seconds = calculate_preparation_time(order_id);
    order.kind = ORDER_KIND_REAL;

    return order;
}

static Order create_stop_order(void)
{
    Order order;

    order.id = -1;
    order.producer_id = -1;
    order.food_type = FOOD_TYPE_PIZZA;
    order.priority = ORDER_PRIORITY_NORMAL;
    order.preparation_seconds = 0;
    order.kind = ORDER_KIND_STOP;

    return order;
}

static int reserve_next_order_id(DeliverySystem *system)
{
    int order_id;

    lock_mutex(&system->order_id_mutex, "bloquear IDs");
    order_id = system->next_order_id++;
    unlock_mutex(&system->order_id_mutex, "desbloquear IDs");

    return order_id;
}

static void increase_counter(int *counter, pthread_mutex_t *mutex)
{
    lock_mutex(mutex, "bloquear contador");
    (*counter)++;
    unlock_mutex(mutex, "desbloquear contador");
}

static void print_line(DeliverySystem *system, const char *message)
{
    lock_mutex(&system->print_mutex, "bloquear impresión");
    printf("%s\n", message);
    fflush(stdout);
    unlock_mutex(&system->print_mutex, "desbloquear impresión");
}

static void log_generated(DeliverySystem *system, int producer_id, const Order *order)
{
    char message[256];
    snprintf(
        message,
        sizeof(message),
        "[Productor %d] Generó pedido %d (%s, %s)",
        producer_id,
        order->id,
        food_type_to_text(order->food_type),
        priority_to_text(order->priority));
    print_line(system, message);
}

static void log_taken(DeliverySystem *system, int cook_id, const Order *order)
{
    char message[256];
    snprintf(
        message,
        sizeof(message),
        "[Cocinero %d] Tomó pedido %d con prioridad %s",
        cook_id,
        order->id,
        priority_to_text(order->priority));
    print_line(system, message);
}

static void log_delivered(DeliverySystem *system, int delivery_id, const Order *order)
{
    char message[256];
    snprintf(
        message,
        sizeof(message),
        "[Repartidor %d] Entregó pedido %d (%s)",
        delivery_id,
        order->id,
        priority_to_text(order->priority));
    print_line(system, message);
}

static void enqueue_stop_orders(OrderQueue *queue, int count)
{
    int index;
    for (index = 0; index < count; index++) {
        order_queue_push(queue, create_stop_order());
    }
}

static void *producer_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;
    int index;

    for (index = 0; index < ORDERS_PER_PRODUCER; index++) {
        int order_id = reserve_next_order_id(context->system);
        Order order = create_order(order_id, context->worker_id);

        log_generated(context->system, context->worker_id, &order);
        order_queue_push(&context->system->pending_orders, order);
        increase_counter(&context->system->generated_orders, &context->system->counters_mutex);
        sleep(GENERATION_PAUSE_SECONDS);
    }

    return NULL;
}

static void *cook_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;

    while (1) {
        Order order = order_queue_pop(&context->system->pending_orders);

        if (order.kind == ORDER_KIND_STOP) {
            return NULL;
        }

        log_taken(context->system, context->worker_id, &order);
        sleep(order.preparation_seconds);
        order_queue_push(&context->system->ready_orders, order);
        increase_counter(&context->system->prepared_orders, &context->system->counters_mutex);
    }
}

static void *delivery_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;

    while (1) {
        Order order = order_queue_pop(&context->system->ready_orders);

        if (order.kind == ORDER_KIND_STOP) {
            return NULL;
        }

        sleep(DELIVERY_SECONDS);
        log_delivered(context->system, context->worker_id, &order);
        increase_counter(&context->system->delivered_orders, &context->system->counters_mutex);
    }
}

static void print_summary(DeliverySystem *system)
{
    lock_mutex(&system->print_mutex, "bloquear impresión");
    printf("\nResumen simulacro 1\n");
    printf("Generados:  %d\n", system->generated_orders);
    printf("Preparados: %d\n", system->prepared_orders);
    printf("Entregados: %d\n", system->delivered_orders);
    unlock_mutex(&system->print_mutex, "desbloquear impresión");
}

int main(void)
{
    DeliverySystem system;
    pthread_t producers[PRODUCER_COUNT];
    pthread_t cooks[COOK_COUNT];
    pthread_t delivery_people[DELIVERY_PERSON_COUNT];
    WorkerContext producer_contexts[PRODUCER_COUNT];
    WorkerContext cook_contexts[COOK_COUNT];
    WorkerContext delivery_contexts[DELIVERY_PERSON_COUNT];
    int index;
    int result = delivery_system_init(&system);

    if (result != 0) {
        fprintf(stderr, "No se pudo iniciar el sistema: %s\n", strerror(result));
        return EXIT_FAILURE;
    }

    print_line(&system, "Simulacro 1: cocineros atienden pedidos URGENTES primero.");

    for (index = 0; index < COOK_COUNT; index++) {
        cook_contexts[index].worker_id = index + 1;
        cook_contexts[index].system = &system;
        result = pthread_create(&cooks[index], NULL, cook_thread, &cook_contexts[index]);
        if (result != 0) {
            finish_with_error("crear cocinero", result);
        }
    }

    for (index = 0; index < DELIVERY_PERSON_COUNT; index++) {
        delivery_contexts[index].worker_id = index + 1;
        delivery_contexts[index].system = &system;
        result = pthread_create(
            &delivery_people[index], NULL, delivery_thread, &delivery_contexts[index]);
        if (result != 0) {
            finish_with_error("crear repartidor", result);
        }
    }

    for (index = 0; index < PRODUCER_COUNT; index++) {
        producer_contexts[index].worker_id = index + 1;
        producer_contexts[index].system = &system;
        result = pthread_create(&producers[index], NULL, producer_thread, &producer_contexts[index]);
        if (result != 0) {
            finish_with_error("crear productor", result);
        }
    }

    for (index = 0; index < PRODUCER_COUNT; index++) {
        result = pthread_join(producers[index], NULL);
        if (result != 0) {
            finish_with_error("esperar productor", result);
        }
    }

    enqueue_stop_orders(&system.pending_orders, COOK_COUNT);

    for (index = 0; index < COOK_COUNT; index++) {
        result = pthread_join(cooks[index], NULL);
        if (result != 0) {
            finish_with_error("esperar cocinero", result);
        }
    }

    enqueue_stop_orders(&system.ready_orders, DELIVERY_PERSON_COUNT);

    for (index = 0; index < DELIVERY_PERSON_COUNT; index++) {
        result = pthread_join(delivery_people[index], NULL);
        if (result != 0) {
            finish_with_error("esperar repartidor", result);
        }
    }

    print_summary(&system);
    delivery_system_destroy(&system);
    return EXIT_SUCCESS;
}
