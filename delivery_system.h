#ifndef DELIVERY_SYSTEM_H
#define DELIVERY_SYSTEM_H

#include <pthread.h>
#include <semaphore.h>

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

int delivery_system_init(DeliverySystem *system);
void delivery_system_destroy(DeliverySystem *system);

int order_queue_init(OrderQueue *queue, const char *name);
void order_queue_destroy(OrderQueue *queue);
void order_queue_push(OrderQueue *queue, Order order);
Order order_queue_pop(OrderQueue *queue);

Order create_order(int order_id, int producer_id);
Order create_stop_order(void);
const char *food_type_to_text(FoodType food_type);
unsigned int calculate_preparation_time(int order_id);

void enqueue_stop_orders(OrderQueue *queue, int stop_order_count);
void print_final_summary(DeliverySystem *system);

void *producer_thread(void *argument);
void *cook_thread(void *argument);
void *delivery_thread(void *argument);

#endif
