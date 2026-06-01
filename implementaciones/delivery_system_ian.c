/*
 * Sistema concurrente de atención de pedidos.
 *
 * Archivo unificado para entrega:
 * este .c junta lo que durante el desarrollo estaba separado en:
 * - delivery_system.h
 * - delivery_system_lib.c
 * - delivery_system.c
 *
 * La separación original sirve para estudiar responsabilidades.
 * La versión unificada sirve para cumplir el enunciado cuando pide entregar
 * un único archivo C llamado delivery_system.c.
 */

#include <errno.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/*
 * Constantes de configuración del TP.
 *
 * La simulación es finita a propósito: si los productores generaran pedidos
 * para siempre, main() nunca podría hacer pthread_join() de forma correcta.
 */
#define PRODUCER_COUNT 3
#define COOK_COUNT 2
#define DELIVERY_PERSON_COUNT 2
#define QUEUE_CAPACITY 5
#define ORDERS_PER_PRODUCER 4

#define MIN_PREPARATION_SECONDS 1
#define MAX_PREPARATION_SECONDS 3
#define DELIVERY_SECONDS 2
#define GENERATION_PAUSE_SECONDS 1

#if PRODUCER_COUNT <= 0
#error "PRODUCER_COUNT debe ser mayor a cero"
#endif

#if COOK_COUNT <= 0
#error "COOK_COUNT debe ser mayor a cero"
#endif

#if DELIVERY_PERSON_COUNT <= 0
#error "DELIVERY_PERSON_COUNT debe ser mayor a cero"
#endif

#if QUEUE_CAPACITY <= 0
#error "QUEUE_CAPACITY debe ser mayor a cero"
#endif

#if ORDERS_PER_PRODUCER <= 0
#error "ORDERS_PER_PRODUCER debe ser mayor a cero"
#endif

#if MAX_PREPARATION_SECONDS < MIN_PREPARATION_SECONDS
#error "MAX_PREPARATION_SECONDS debe ser mayor o igual a MIN_PREPARATION_SECONDS"
#endif

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
    const char *name;

    /*
     * El mutex protege head, tail, count y el arreglo items.
     * Los semáforos evitan busy waiting:
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

static void exit_with_pthread_error(const char *operation, int error_code);
static void exit_if_pthread_failed(int error_code, const char *operation);
static void lock_mutex(pthread_mutex_t *mutex, const char *mutex_name);
static void unlock_mutex(pthread_mutex_t *mutex, const char *mutex_name);
static void wait_for_semaphore(sem_t *semaphore, const char *semaphore_name);
static void post_to_semaphore(sem_t *semaphore, const char *semaphore_name);

static int delivery_system_init(DeliverySystem *system);
static void delivery_system_destroy(DeliverySystem *system);

static int order_queue_init(OrderQueue *queue, const char *name);
static void order_queue_destroy(OrderQueue *queue);
static void order_queue_push(OrderQueue *queue, Order order);
static Order order_queue_pop(OrderQueue *queue);

static int reserve_next_order_id(DeliverySystem *system);
static Order create_order(int order_id, int producer_id);
static Order create_stop_order(void);
static const char *food_type_to_text(FoodType food_type);
static unsigned int calculate_preparation_time(int order_id);

static void register_generated_order(DeliverySystem *system);
static void register_prepared_order(DeliverySystem *system);
static void register_delivered_order(DeliverySystem *system);

static void print_with_mutex(DeliverySystem *system, const char *message);
static void log_order_generated(DeliverySystem *system, int producer_id, const Order *order);
static void log_order_taken_by_cook(DeliverySystem *system, int cook_id, const Order *order);
static void log_order_delivered(DeliverySystem *system, int delivery_person_id, const Order *order);
static void enqueue_stop_orders(OrderQueue *queue, int stop_order_count);
static void print_startup_configuration(void);
static void print_final_summary(DeliverySystem *system);

static void *producer_thread(void *argument);
static void *cook_thread(void *argument);
static void *delivery_thread(void *argument);

static void exit_with_pthread_error(const char *operation, int error_code)
{
    fprintf(stderr, "Error en %s: %s\n", operation, strerror(error_code));
    exit(EXIT_FAILURE);
}

static void exit_if_pthread_failed(int error_code, const char *operation)
{
    if (error_code != 0) {
        exit_with_pthread_error(operation, error_code);
    }
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

static int delivery_system_init(DeliverySystem *system)
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

static void delivery_system_destroy(DeliverySystem *system)
{
    pthread_mutex_destroy(&system->print_mutex);
    pthread_mutex_destroy(&system->counters_mutex);
    pthread_mutex_destroy(&system->order_id_mutex);
    order_queue_destroy(&system->ready_orders);
    order_queue_destroy(&system->pending_orders);
}

static int order_queue_init(OrderQueue *queue, const char *name)
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

static void order_queue_destroy(OrderQueue *queue)
{
    sem_destroy(&queue->available_items);
    sem_destroy(&queue->available_slots);
    pthread_mutex_destroy(&queue->mutex);
}

static void order_queue_push(OrderQueue *queue, Order order)
{
    wait_for_semaphore(&queue->available_slots, "esperar espacio libre en cola");

    lock_mutex(&queue->mutex, "bloquear mutex de cola");
    queue->items[queue->tail] = order;
    queue->tail = (queue->tail + 1) % QUEUE_CAPACITY;
    queue->count++;
    unlock_mutex(&queue->mutex, "desbloquear mutex de cola");

    post_to_semaphore(&queue->available_items, "avisar item disponible en cola");
}

static Order order_queue_pop(OrderQueue *queue)
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

static int reserve_next_order_id(DeliverySystem *system)
{
    int order_id;

    lock_mutex(&system->order_id_mutex, "bloquear mutex de IDs");
    order_id = system->next_order_id;
    system->next_order_id++;
    unlock_mutex(&system->order_id_mutex, "desbloquear mutex de IDs");

    return order_id;
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
        return "Comida desconocida";
    }
}

static unsigned int calculate_preparation_time(int order_id)
{
    const unsigned int preparation_range =
        MAX_PREPARATION_SECONDS - MIN_PREPARATION_SECONDS + 1;

    return MIN_PREPARATION_SECONDS + (unsigned int)((order_id - 1) % preparation_range);
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

static void enqueue_stop_orders(OrderQueue *queue, int stop_order_count)
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

static void print_startup_configuration(void)
{
    printf("Sistema concurrente de atención de pedidos\n");
    printf("------------------------------------------\n");
    printf("Productores:              %d\n", PRODUCER_COUNT);
    printf("Cocineros:                %d\n", COOK_COUNT);
    printf("Repartidores:             %d\n", DELIVERY_PERSON_COUNT);
    printf("Capacidad de cada cola:   %d\n", QUEUE_CAPACITY);
    printf("Pedidos por productor:    %d\n\n", ORDERS_PER_PRODUCER);
}

static void print_final_summary(DeliverySystem *system)
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

static void *producer_thread(void *argument)
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

static void *cook_thread(void *argument)
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

static void *delivery_thread(void *argument)
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

int main(void)
{
    DeliverySystem system;
    pthread_t producer_threads[PRODUCER_COUNT];
    pthread_t cook_threads[COOK_COUNT];
    pthread_t delivery_threads[DELIVERY_PERSON_COUNT];
    WorkerContext producer_contexts[PRODUCER_COUNT];
    WorkerContext cook_contexts[COOK_COUNT];
    WorkerContext delivery_contexts[DELIVERY_PERSON_COUNT];
    int index;
    int result;

    result = delivery_system_init(&system);
    if (result != 0) {
        fprintf(stderr, "No se pudo inicializar el sistema: %s\n", strerror(result));
        return EXIT_FAILURE;
    }

    print_startup_configuration();

    /*
     * Los consumidores se crean primero para que ya estén esperando en sus
     * semáforos cuando los productores empiecen a cargar pedidos.
     */
    for (index = 0; index < COOK_COUNT; index++) {
        cook_contexts[index].worker_id = index + 1;
        cook_contexts[index].system = &system;

        exit_if_pthread_failed(
            pthread_create(&cook_threads[index], NULL, cook_thread, &cook_contexts[index]),
            "crear thread cocinero");
    }

    for (index = 0; index < DELIVERY_PERSON_COUNT; index++) {
        delivery_contexts[index].worker_id = index + 1;
        delivery_contexts[index].system = &system;

        exit_if_pthread_failed(
            pthread_create(
                &delivery_threads[index],
                NULL,
                delivery_thread,
                &delivery_contexts[index]),
            "crear thread repartidor");
    }

    for (index = 0; index < PRODUCER_COUNT; index++) {
        producer_contexts[index].worker_id = index + 1;
        producer_contexts[index].system = &system;

        exit_if_pthread_failed(
            pthread_create(
                &producer_threads[index],
                NULL,
                producer_thread,
                &producer_contexts[index]),
            "crear thread productor");
    }

    for (index = 0; index < PRODUCER_COUNT; index++) {
        exit_if_pthread_failed(pthread_join(producer_threads[index], NULL), "esperar productor");
    }

    /*
     * Cuando no habrá más pedidos nuevos, main inserta un centinela por
     * cocinero. Como la cola es FIFO, primero se procesan los pedidos reales
     * que ya estaban pendientes y recién después cada cocinero recibe su cierre.
     */
    enqueue_stop_orders(&system.pending_orders, COOK_COUNT);

    for (index = 0; index < COOK_COUNT; index++) {
        exit_if_pthread_failed(pthread_join(cook_threads[index], NULL), "esperar cocinero");
    }

    /*
     * Cuando todos los cocineros terminaron, ya no se agregarán pedidos listos.
     * Ahora se envía un centinela por repartidor para cerrar la última etapa.
     */
    enqueue_stop_orders(&system.ready_orders, DELIVERY_PERSON_COUNT);

    for (index = 0; index < DELIVERY_PERSON_COUNT; index++) {
        exit_if_pthread_failed(
            pthread_join(delivery_threads[index], NULL),
            "esperar repartidor");
    }

    print_final_summary(&system);
    delivery_system_destroy(&system);

    return EXIT_SUCCESS;
}
