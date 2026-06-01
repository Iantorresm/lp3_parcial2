/*
 * Simulacro 6 - Separación estricta entre pedidos pendientes y pedidos listos.
 *
 * Enunciado del simulacro:
 * el sistema debe trabajar con dos colas independientes:
 * - pendientes: pedidos que todavía esperan preparación.
 * - listos: pedidos ya preparados y disponibles para entrega.
 *
 * Modificación requerida:
 * los cocineros SOLO deben consumir pedidos desde la cola de pendientes y,
 * luego de prepararlos, SOLO deben moverlos hacia la cola de listos.
 * Los repartidores SOLO deben consumir pedidos desde la cola de listos.
 *
 * Concepto evaluado:
 * separación de responsabilidades entre etapas del pipeline concurrente.
 * Si un repartidor mira pendientes, o si un cocinero consume listos, el diseño
 * está mezclando responsabilidades y rompe el flujo Generación -> Preparación
 * -> Entrega.
 */

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define COOK_COUNT 2
#define DELIVERY_PERSON_COUNT 2
#define INITIAL_PENDING_ORDERS 10
#define PENDING_QUEUE_CAPACITY 5
#define READY_QUEUE_CAPACITY 5

#define MIN_PREPARATION_SECONDS 1
#define MAX_PREPARATION_SECONDS 3
#define DELIVERY_SECONDS 1

typedef enum {
    ORDER_KIND_REAL = 0,
    ORDER_KIND_STOP
} OrderKind;

typedef struct {
    int id;
    unsigned int preparation_seconds;
    OrderKind kind;
} Order;

typedef struct {
    Order *items;
    int capacity;
    int head;
    int tail;
    int count;
    const char *name;

    /*
     * Cada cola se protege a sí misma.
     * - mutex protege head, tail, count e items.
     * - available_slots bloquea cuando la cola está llena.
     * - available_items bloquea cuando la cola está vacía.
     */
    pthread_mutex_t mutex;
    sem_t available_slots;
    sem_t available_items;
} OrderQueue;

typedef struct {
    OrderQueue pending_orders;
    OrderQueue ready_orders;
    int prepared_orders;
    int delivered_orders;
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

static int order_queue_init(OrderQueue *queue, int capacity, const char *name)
{
    int result;

    queue->items = calloc((size_t)capacity, sizeof(Order));
    if (queue->items == NULL) {
        return ENOMEM;
    }

    queue->capacity = capacity;
    queue->head = 0;
    queue->tail = 0;
    queue->count = 0;
    queue->name = name;

    result = pthread_mutex_init(&queue->mutex, NULL);
    if (result != 0) {
        free(queue->items);
        return result;
    }

    if (sem_init(&queue->available_slots, 0, (unsigned int)capacity) == -1) {
        result = errno;
        pthread_mutex_destroy(&queue->mutex);
        free(queue->items);
        return result;
    }

    if (sem_init(&queue->available_items, 0, 0) == -1) {
        result = errno;
        sem_destroy(&queue->available_slots);
        pthread_mutex_destroy(&queue->mutex);
        free(queue->items);
        return result;
    }

    return 0;
}

static void order_queue_destroy(OrderQueue *queue)
{
    sem_destroy(&queue->available_items);
    sem_destroy(&queue->available_slots);
    pthread_mutex_destroy(&queue->mutex);
    free(queue->items);
}

static void order_queue_push(OrderQueue *queue, Order order)
{
    wait_sem(&queue->available_slots, "esperar espacio libre en cola");

    lock_mutex(&queue->mutex, "bloquear cola");
    queue->items[queue->tail] = order;
    queue->tail = (queue->tail + 1) % queue->capacity;
    queue->count++;
    unlock_mutex(&queue->mutex, "desbloquear cola");

    post_sem(&queue->available_items, "avisar item disponible en cola");
}

static Order order_queue_pop(OrderQueue *queue)
{
    Order order;

    wait_sem(&queue->available_items, "esperar item disponible en cola");

    lock_mutex(&queue->mutex, "bloquear cola");
    order = queue->items[queue->head];
    queue->head = (queue->head + 1) % queue->capacity;
    queue->count--;
    unlock_mutex(&queue->mutex, "desbloquear cola");

    post_sem(&queue->available_slots, "avisar espacio libre en cola");

    return order;
}

static int delivery_system_init(DeliverySystem *system)
{
    int result;

    system->prepared_orders = 0;
    system->delivered_orders = 0;

    result = order_queue_init(
        &system->pending_orders,
        PENDING_QUEUE_CAPACITY,
        "pendientes");
    if (result != 0) {
        return result;
    }

    result = order_queue_init(&system->ready_orders, READY_QUEUE_CAPACITY, "listos");
    if (result != 0) {
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    result = pthread_mutex_init(&system->counters_mutex, NULL);
    if (result != 0) {
        order_queue_destroy(&system->ready_orders);
        order_queue_destroy(&system->pending_orders);
        return result;
    }

    result = pthread_mutex_init(&system->print_mutex, NULL);
    if (result != 0) {
        pthread_mutex_destroy(&system->counters_mutex);
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
    order_queue_destroy(&system->ready_orders);
    order_queue_destroy(&system->pending_orders);
}

static unsigned int calculate_preparation_seconds(int order_id)
{
    return MIN_PREPARATION_SECONDS +
           (unsigned int)((order_id - 1) %
                          (MAX_PREPARATION_SECONDS - MIN_PREPARATION_SECONDS + 1));
}

static Order create_order(int order_id)
{
    Order order;

    order.id = order_id;
    order.preparation_seconds = calculate_preparation_seconds(order_id);
    order.kind = ORDER_KIND_REAL;

    return order;
}

static Order create_stop_order(void)
{
    Order order;

    order.id = -1;
    order.preparation_seconds = 0;
    order.kind = ORDER_KIND_STOP;

    return order;
}

static void print_event(DeliverySystem *system, const char *message)
{
    lock_mutex(&system->print_mutex, "bloquear impresión");
    printf("%s\n", message);
    fflush(stdout);
    unlock_mutex(&system->print_mutex, "desbloquear impresión");
}

static void increment_prepared_orders(DeliverySystem *system)
{
    lock_mutex(&system->counters_mutex, "bloquear contadores");
    system->prepared_orders++;
    unlock_mutex(&system->counters_mutex, "desbloquear contadores");
}

static void increment_delivered_orders(DeliverySystem *system)
{
    lock_mutex(&system->counters_mutex, "bloquear contadores");
    system->delivered_orders++;
    unlock_mutex(&system->counters_mutex, "desbloquear contadores");
}

static void enqueue_stop_orders(OrderQueue *queue, int stop_order_count)
{
    int index;

    for (index = 0; index < stop_order_count; index++) {
        order_queue_push(queue, create_stop_order());
    }
}

static void enqueue_initial_pending_orders(DeliverySystem *system)
{
    int order_id;

    for (order_id = 1; order_id <= INITIAL_PENDING_ORDERS; order_id++) {
        char message[128];
        Order order = create_order(order_id);

        snprintf(
            message,
            sizeof(message),
            "[Sistema] Pedido %d entra en cola PENDIENTES",
            order.id);
        print_event(system, message);

        order_queue_push(&system->pending_orders, order);
    }
}

static void *cook_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;
    DeliverySystem *system = context->system;

    while (1) {
        char message[192];
        Order order = order_queue_pop(&system->pending_orders);

        if (order.kind == ORDER_KIND_STOP) {
            return NULL;
        }

        snprintf(
            message,
            sizeof(message),
            "[Cocinero %d] Toma pedido %d SOLO desde PENDIENTES",
            context->worker_id,
            order.id);
        print_event(system, message);

        sleep(order.preparation_seconds);

        snprintf(
            message,
            sizeof(message),
            "[Cocinero %d] Pedido %d preparado; lo mueve a LISTOS",
            context->worker_id,
            order.id);
        print_event(system, message);

        order_queue_push(&system->ready_orders, order);
        increment_prepared_orders(system);
    }
}

static void *delivery_thread(void *argument)
{
    WorkerContext *context = (WorkerContext *)argument;
    DeliverySystem *system = context->system;

    while (1) {
        char message[192];
        Order order = order_queue_pop(&system->ready_orders);

        if (order.kind == ORDER_KIND_STOP) {
            return NULL;
        }

        snprintf(
            message,
            sizeof(message),
            "[Delivery %d] Retira pedido %d SOLO desde LISTOS",
            context->worker_id,
            order.id);
        print_event(system, message);

        sleep(DELIVERY_SECONDS);

        snprintf(
            message,
            sizeof(message),
            "[Delivery %d] Entrega pedido %d",
            context->worker_id,
            order.id);
        print_event(system, message);

        increment_delivered_orders(system);
    }
}

static void print_summary(DeliverySystem *system)
{
    int prepared_orders;
    int delivered_orders;

    lock_mutex(&system->counters_mutex, "bloquear contadores");
    prepared_orders = system->prepared_orders;
    delivered_orders = system->delivered_orders;
    unlock_mutex(&system->counters_mutex, "desbloquear contadores");

    lock_mutex(&system->print_mutex, "bloquear impresión");
    printf("\nResumen simulacro 6\n");
    printf("Pedidos iniciales en pendientes: %d\n", INITIAL_PENDING_ORDERS);
    printf("Pedidos preparados:             %d\n", prepared_orders);
    printf("Pedidos entregados:             %d\n", delivered_orders);
    printf("\nRegla verificada por diseño:\n");
    printf("- Cocineros consumen pendientes y producen listos.\n");
    printf("- Delivery consume listos y nunca mira pendientes.\n");
    unlock_mutex(&system->print_mutex, "desbloquear impresión");
}

int main(void)
{
    DeliverySystem system;
    pthread_t cook_threads[COOK_COUNT];
    pthread_t delivery_threads[DELIVERY_PERSON_COUNT];
    WorkerContext cook_contexts[COOK_COUNT];
    WorkerContext delivery_contexts[DELIVERY_PERSON_COUNT];
    int index;
    int result = delivery_system_init(&system);

    if (result != 0) {
        fprintf(stderr, "No se pudo iniciar el sistema: %s\n", strerror(result));
        return EXIT_FAILURE;
    }

    print_event(
        &system,
        "Simulacro 6: cocineros miran PENDIENTES; delivery mira LISTOS.");

    for (index = 0; index < COOK_COUNT; index++) {
        cook_contexts[index].worker_id = index + 1;
        cook_contexts[index].system = &system;

        result = pthread_create(
            &cook_threads[index],
            NULL,
            cook_thread,
            &cook_contexts[index]);
        if (result != 0) {
            fail_pthread("crear cocinero", result);
        }
    }

    for (index = 0; index < DELIVERY_PERSON_COUNT; index++) {
        delivery_contexts[index].worker_id = index + 1;
        delivery_contexts[index].system = &system;

        result = pthread_create(
            &delivery_threads[index],
            NULL,
            delivery_thread,
            &delivery_contexts[index]);
        if (result != 0) {
            fail_pthread("crear delivery", result);
        }
    }

    enqueue_initial_pending_orders(&system);

    /*
     * Los centinelas de pendientes solo cierran cocineros.
     * Los delivery no reciben estos centinelas porque no consumen pendientes.
     */
    enqueue_stop_orders(&system.pending_orders, COOK_COUNT);

    for (index = 0; index < COOK_COUNT; index++) {
        result = pthread_join(cook_threads[index], NULL);
        if (result != 0) {
            fail_pthread("esperar cocinero", result);
        }
    }

    /*
     * Recién cuando todos los cocineros terminaron, sabemos que nadie agregará
     * más pedidos a listos. Ahí sí mandamos centinelas a delivery.
     */
    enqueue_stop_orders(&system.ready_orders, DELIVERY_PERSON_COUNT);

    for (index = 0; index < DELIVERY_PERSON_COUNT; index++) {
        result = pthread_join(delivery_threads[index], NULL);
        if (result != 0) {
            fail_pthread("esperar delivery", result);
        }
    }

    print_summary(&system);
    delivery_system_destroy(&system);

    return EXIT_SUCCESS;
}
