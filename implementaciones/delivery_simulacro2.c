/*
 * Simulacro 2 - Hornallas limitadas.
 *
 * Enunciado del simulacro:
 * el restaurante tiene menos hornallas disponibles que cocineros trabajando.
 *
 * Modificación requerida:
 * antes de preparar un pedido, cada cocinero debe esperar una hornalla libre
 * con sem_wait(); al terminar, debe liberarla con sem_post().
 *
 * Concepto evaluado:
 * uso de semáforos como contador de recursos compartidos limitados.
 */

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define PRODUCER_COUNT 3
#define COOK_COUNT 4
#define DELIVERY_PERSON_COUNT 2
#define BURNER_COUNT 2
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
    ORDER_KIND_REAL = 0,
    ORDER_KIND_STOP
} OrderKind;

typedef struct {
    int id;
    int producer_id;
    FoodType food_type;
    unsigned int preparation_seconds;
    OrderKind kind;
} Order;

typedef struct {
    Order items[QUEUE_CAPACITY];
    int head;
    int tail;
    int count;
    pthread_mutex_t mutex;
    sem_t available_slots;
    sem_t available_items;
} OrderQueue;

typedef struct {
    OrderQueue pending_orders;
    OrderQueue ready_orders;
    sem_t available_burners;
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

static void fail_pthread(const char *operation, int error_code)
{
    fprintf(stderr, "Error en %s: %s\n", operation, strerror(error_code));
    exit(EXIT_FAILURE);
}

static void lock_mutex(pthread_mutex_t *mutex, const char *operation)
{
    int result = pthread_mutex_lock(mutex);
    if (result != 0) {
        fail_pthread(operation, result);
    }
}

static void unlock_mutex(pthread_mutex_t *mutex, const char *operation)
{
    int result = pthread_mutex_unlock(mutex);
    if (result != 0) {
        fail_pthread(operation, result);
    }
}

static void wait_sem(sem_t *semaphore, const char *operation)
{
    while (sem_wait(semaphore) == -1) {
        if (errno == EINTR) {
            continue;
        }
        perror(operation);
        exit(EXIT_FAILURE);
    }
}

static void post_sem(sem_t *semaphore, const char *operation)
{
    if (sem_post(semaphore) == -1) {
        perror(operation);
        exit(EXIT_FAILURE);
    }
}

static int order_queue_init(OrderQueue *queue)
{
    int result;

    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;

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
    wait_sem(&queue->available_slots, "esperar espacio en cola");
    lock_mutex(&queue->mutex, "bloquear cola");
    queue->items[queue->tail] = order;
    queue->tail = (queue->tail + 1) % QUEUE_CAPACITY;
    queue->count++;
    unlock_mutex(&queue->mutex, "desbloquear cola");
    post_sem(&queue->available_items, "avisar item en cola");
}

static Order order_queue_pop(OrderQueue *queue)
{
    Order order;

    wait_sem(&queue->available_items, "esperar item en cola");
    lock_mutex(&queue->mutex, "bloquear cola");
    order = queue->items[queue->head];
    queue->head = (queue->head + 1) % QUEUE_CAPACITY;
    queue->count--;
    unlock_mutex(&queue->mutex, "desbloquear cola");
    post_sem(&queue->available_slots, "avisar espacio en cola");

    return order;
}

static int delivery_system_init(DeliverySystem *system)
{
    int result;

    system->next_order_id = 1;
    system->generated_orders = 0;
    system->prepared_orders = 0;
    system->delivered_orders = 0;

    result = order_queue_init(&system->pending_orders);
    if (result != 0) {
        return result;
    }

    result = order_queue_init(&system->ready_orders);
    if (result != 0) {
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    if (sem_init(&system->available_burners, 0, BURNER_COUNT) == -1) {
        result = errno;
        order_queue_destroy(&system->ready_orders);
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    result = pthread_mutex_init(&system->order_id_mutex, NULL);
    if (result != 0) {
        sem_destroy(&system->available_burners);
        order_queue_destroy(&system->ready_orders);
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    result = pthread_mutex_init(&system->counters_mutex, NULL);
    if (result != 0) {
        pthread_mutex_destroy(&system->order_id_mutex);
        sem_destroy(&system->available_burners);
        order_queue_destroy(&system->ready_orders);
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    result = pthread_mutex_init(&system->print_mutex, NULL);
    if (result != 0) {
        pthread_mutex_destroy(&system->counters_mutex);
        pthread_mutex_destroy(&system->order_id_mutex);
        sem_destroy(&system->available_burners);
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
    sem_destroy(&system->available_burners);
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
    order.preparation_seconds = 0;
    order.kind = ORDER_KIND_STOP;
    return order;
}

static int next_order_id(DeliverySystem *system)
{
    int id;
    lock_mutex(&system->order_id_mutex, "bloquear IDs");
    id = system->next_order_id++;
    unlock_mutex(&system->order_id_mutex, "desbloquear IDs");
    return id;
}

static void increment_counter(int *counter, pthread_mutex_t *mutex)
{
    lock_mutex(mutex, "bloquear contador");
    (*counter)++;
    unlock_mutex(mutex, "desbloquear contador");
}

static void print_event(DeliverySystem *system, const char *message)
{
    lock_mutex(&system->print_mutex, "bloquear impresión");
    printf("%s\n", message);
    fflush(stdout);
    unlock_mutex(&system->print_mutex, "desbloquear impresión");
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
        char message[256];
        int id = next_order_id(context->system);
        Order order = create_order(id, context->worker_id);

        snprintf(
            message,
            sizeof(message),
            "[Productor %d] Generó pedido %d (%s)",
            context->worker_id,
            order.id,
            food_type_to_text(order.food_type));
        print_event(context->system, message);
        order_queue_push(&context->system->pending_orders, order);
        increment_counter(&context->system->generated_orders, &context->system->counters_mutex);
        sleep(GENERATION_PAUSE_SECONDS);
    }

    return NULL;
}

static void *cook_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;

    while (1) {
        char message[256];
        Order order = order_queue_pop(&context->system->pending_orders);

        if (order.kind == ORDER_KIND_STOP) {
            return NULL;
        }

        snprintf(
            message,
            sizeof(message),
            "[Cocinero %d] Espera hornalla para pedido %d",
            context->worker_id,
            order.id);
        print_event(context->system, message);

        wait_sem(&context->system->available_burners, "esperar hornalla disponible");

        snprintf(
            message,
            sizeof(message),
            "[Cocinero %d] Usa hornalla para pedido %d",
            context->worker_id,
            order.id);
        print_event(context->system, message);

        sleep(order.preparation_seconds);

        snprintf(
            message,
            sizeof(message),
            "[Cocinero %d] Libera hornalla del pedido %d",
            context->worker_id,
            order.id);
        print_event(context->system, message);

        post_sem(&context->system->available_burners, "liberar hornalla");
        order_queue_push(&context->system->ready_orders, order);
        increment_counter(&context->system->prepared_orders, &context->system->counters_mutex);
    }
}

static void *delivery_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;

    while (1) {
        char message[256];
        Order order = order_queue_pop(&context->system->ready_orders);

        if (order.kind == ORDER_KIND_STOP) {
            return NULL;
        }

        sleep(DELIVERY_SECONDS);
        snprintf(
            message,
            sizeof(message),
            "[Repartidor %d] Entregó pedido %d",
            context->worker_id,
            order.id);
        print_event(context->system, message);
        increment_counter(&context->system->delivered_orders, &context->system->counters_mutex);
    }
}

static void print_summary(DeliverySystem *system)
{
    lock_mutex(&system->print_mutex, "bloquear impresión");
    printf("\nResumen simulacro 2\n");
    printf("Hornallas disponibles: %d\n", BURNER_COUNT);
    printf("Cocineros:             %d\n", COOK_COUNT);
    printf("Generados:             %d\n", system->generated_orders);
    printf("Preparados:            %d\n", system->prepared_orders);
    printf("Entregados:            %d\n", system->delivered_orders);
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

    print_event(&system, "Simulacro 2: cocineros compiten por hornallas limitadas.");

    for (index = 0; index < COOK_COUNT; index++) {
        cook_contexts[index].worker_id = index + 1;
        cook_contexts[index].system = &system;
        result = pthread_create(&cooks[index], NULL, cook_thread, &cook_contexts[index]);
        if (result != 0) {
            fail_pthread("crear cocinero", result);
        }
    }

    for (index = 0; index < DELIVERY_PERSON_COUNT; index++) {
        delivery_contexts[index].worker_id = index + 1;
        delivery_contexts[index].system = &system;
        result = pthread_create(
            &delivery_people[index], NULL, delivery_thread, &delivery_contexts[index]);
        if (result != 0) {
            fail_pthread("crear repartidor", result);
        }
    }

    for (index = 0; index < PRODUCER_COUNT; index++) {
        producer_contexts[index].worker_id = index + 1;
        producer_contexts[index].system = &system;
        result = pthread_create(&producers[index], NULL, producer_thread, &producer_contexts[index]);
        if (result != 0) {
            fail_pthread("crear productor", result);
        }
    }

    for (index = 0; index < PRODUCER_COUNT; index++) {
        result = pthread_join(producers[index], NULL);
        if (result != 0) {
            fail_pthread("esperar productor", result);
        }
    }

    enqueue_stop_orders(&system.pending_orders, COOK_COUNT);

    for (index = 0; index < COOK_COUNT; index++) {
        result = pthread_join(cooks[index], NULL);
        if (result != 0) {
            fail_pthread("esperar cocinero", result);
        }
    }

    enqueue_stop_orders(&system.ready_orders, DELIVERY_PERSON_COUNT);

    for (index = 0; index < DELIVERY_PERSON_COUNT; index++) {
        result = pthread_join(delivery_people[index], NULL);
        if (result != 0) {
            fail_pthread("esperar repartidor", result);
        }
    }

    print_summary(&system);
    delivery_system_destroy(&system);
    return EXIT_SUCCESS;
}
