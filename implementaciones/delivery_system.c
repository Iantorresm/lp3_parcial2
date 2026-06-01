#include "delivery_system.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void exit_if_pthread_failed(int error_code, const char *operation)
{
    if (error_code != 0) {
        fprintf(stderr, "Error en %s: %s\n", operation, strerror(error_code));
        exit(EXIT_FAILURE);
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
