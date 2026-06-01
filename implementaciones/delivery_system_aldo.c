/*
 * delivery_system.c
 * Trabajo Practico LP3 - POSIX Threads
 * Sistema Concurrente de Atencion de Pedidos
 */

#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

// Constantes
#define NUM_PRODUCTORES 3
#define NUM_COCINEROS 2
#define NUM_REPARTIDORES 2
#define TAM_COLA_PENDIENTES 5
#define TAM_COLA_LISTOS 5
#define TOTAL_PEDIDOS 20

#define TIEMPO_ENTRE_PEDIDOS 1
#define TIEMPO_ENTREGA 2

// Estructuras
typedef struct
{
    int id;
    int tiempo_preparacion;
    char tipo_comida[30];
} Pedido;

typedef struct
{
    Pedido datos[TAM_COLA_PENDIENTES > TAM_COLA_LISTOS ? TAM_COLA_PENDIENTES : TAM_COLA_LISTOS];
    int frente;
    int final;
    int cantidad;
    int capacidad;
} ColaPedidos;

// Variables globales
ColaPedidos cola_pendientes;
ColaPedidos cola_listos;

int siguiente_id = 1;
int pedidos_generados = 0;
int pedidos_entregados = 0;
int productores_terminados = 0;
int cocineros_terminados = 0;

// mutex para proteger variables compartidas
pthread_mutex_t mutex_id;
pthread_mutex_t mutex_generados;
pthread_mutex_t mutex_entregados;
pthread_mutex_t mutex_productores_terminados;
pthread_mutex_t mutex_cocineros_terminados;
pthread_mutex_t mutex_cola_pendientes;
pthread_mutex_t mutex_cola_listos;
pthread_mutex_t mutex_print;

// semaforos para controlar el acceso a las colas
sem_t espacios_pendientes;
sem_t pedidos_pendientes;
sem_t espacios_listos;
sem_t pedidos_listos;

// funciones de cola
void inicializar_cola(ColaPedidos *cola, int capacidad)
{
    cola->frente = 0;
    cola->final = 0;
    cola->cantidad = 0;
    cola->capacidad = capacidad;
}

void insertar_cola(ColaPedidos *cola, Pedido pedido)
{
    cola->datos[cola->final] = pedido;
    cola->final = (cola->final + 1) % cola->capacidad;
    cola->cantidad++;
}

Pedido retirar_cola(ColaPedidos *cola)
{
    Pedido pedido = cola->datos[cola->frente];
    cola->frente = (cola->frente + 1) % cola->capacidad;
    cola->cantidad--;
    return pedido;
}

// utilidades
void imprimir_evento(const char *mensaje)
{
    pthread_mutex_lock(&mutex_print);
    printf("%s\n", mensaje);
    fflush(stdout);
    pthread_mutex_unlock(&mutex_print);
}

void obtener_tipo_comida(char *destino, size_t tam, int id)
{
    const char *tipos[] = {
        "Hamburguesa",
        "Pizza",
        "Milanesa",
        "Lomito",
        "Empanadas",
        "Pasta"};

    int cantidad_tipos = sizeof(tipos) / sizeof(tipos[0]);
    snprintf(destino, tam, "%s", tipos[id % cantidad_tipos]);
}

int obtener_siguiente_id(void)
{
    int id;

    pthread_mutex_lock(&mutex_id);
    id = siguiente_id++;
    pthread_mutex_unlock(&mutex_id);

    return id;
}

int intentar_reservar_pedido_a_generar(void)
{
    int puede_generar = 0;

    pthread_mutex_lock(&mutex_generados);
    if (pedidos_generados < TOTAL_PEDIDOS)
    {
        pedidos_generados++;
        puede_generar = 1;
    }
    pthread_mutex_unlock(&mutex_generados);

    return puede_generar;
}

int obtener_pedidos_entregados(void)
{
    int total;

    pthread_mutex_lock(&mutex_entregados);
    total = pedidos_entregados;
    pthread_mutex_unlock(&mutex_entregados);

    return total;
}

void incrementar_pedidos_entregados(void)
{
    pthread_mutex_lock(&mutex_entregados);
    pedidos_entregados++;
    pthread_mutex_unlock(&mutex_entregados);
}

void marcar_productor_terminado(void)
{
    pthread_mutex_lock(&mutex_productores_terminados);
    productores_terminados++;

    if (productores_terminados == NUM_PRODUCTORES)
    {
        for (int i = 0; i < NUM_COCINEROS; i++)
        {
            sem_post(&pedidos_pendientes);
        }
    }

    pthread_mutex_unlock(&mutex_productores_terminados);
}

int todos_los_productores_terminaron(void)
{
    int terminaron;

    pthread_mutex_lock(&mutex_productores_terminados);
    terminaron = (productores_terminados == NUM_PRODUCTORES);
    pthread_mutex_unlock(&mutex_productores_terminados);

    return terminaron;
}

void marcar_cocinero_terminado(void)
{
    pthread_mutex_lock(&mutex_cocineros_terminados);
    cocineros_terminados++;

    if (cocineros_terminados == NUM_COCINEROS)
    {
        for (int i = 0; i < NUM_REPARTIDORES; i++)
        {
            sem_post(&pedidos_listos);
        }
    }

    pthread_mutex_unlock(&mutex_cocineros_terminados);
}

int todos_los_cocineros_terminaron(void)
{
    int terminaron;

    pthread_mutex_lock(&mutex_cocineros_terminados);
    terminaron = (cocineros_terminados == NUM_COCINEROS);
    pthread_mutex_unlock(&mutex_cocineros_terminados);

    return terminaron;
}

/* ================= THREADS PRODUCTORES ================= */
void *productor(void *arg)
{
    int id_productor = *(int *)arg;

    while (intentar_reservar_pedido_a_generar())
    {
        Pedido pedido;
        char mensaje[160];

        pedido.id = obtener_siguiente_id();
        pedido.tiempo_preparacion = 1 + (pedido.id % 3);
        obtener_tipo_comida(pedido.tipo_comida, sizeof(pedido.tipo_comida), pedido.id);

        sleep(TIEMPO_ENTRE_PEDIDOS);

        sem_wait(&espacios_pendientes);
        pthread_mutex_lock(&mutex_cola_pendientes);

        insertar_cola(&cola_pendientes, pedido);

        pthread_mutex_unlock(&mutex_cola_pendientes);
        sem_post(&pedidos_pendientes);

        snprintf(mensaje, sizeof(mensaje),
                 "[PRODUCTOR %d] Pedido generado -> ID: %d | Comida: %s | Preparacion: %d seg",
                 id_productor, pedido.id, pedido.tipo_comida, pedido.tiempo_preparacion);
        imprimir_evento(mensaje);
    }

    char mensaje[80];
    snprintf(mensaje, sizeof(mensaje), "[PRODUCTOR %d] No genera mas pedidos.", id_productor);
    imprimir_evento(mensaje);

    marcar_productor_terminado();
    return NULL;
}

// threads cocineros
void *cocinero(void *arg)
{
    int id_cocinero = *(int *)arg;

    while (1)
    {
        Pedido pedido;
        char mensaje[180];
        int hay_pedido = 0;

        sem_wait(&pedidos_pendientes);
        pthread_mutex_lock(&mutex_cola_pendientes);

        if (cola_pendientes.cantidad > 0)
        {
            pedido = retirar_cola(&cola_pendientes);
            hay_pedido = 1;
        }

        pthread_mutex_unlock(&mutex_cola_pendientes);

        if (!hay_pedido)
        {
            if (todos_los_productores_terminaron())
            {
                break;
            }
            continue;
        }

        sem_post(&espacios_pendientes);

        snprintf(mensaje, sizeof(mensaje),
                 "[COCINERO %d] Pedido tomado -> ID: %d | Comida: %s",
                 id_cocinero, pedido.id, pedido.tipo_comida);
        imprimir_evento(mensaje);

        sleep(pedido.tiempo_preparacion);

        sem_wait(&espacios_listos);
        pthread_mutex_lock(&mutex_cola_listos);

        insertar_cola(&cola_listos, pedido);

        pthread_mutex_unlock(&mutex_cola_listos);
        sem_post(&pedidos_listos);

        snprintf(mensaje, sizeof(mensaje),
                 "[COCINERO %d] Pedido listo para entrega -> ID: %d",
                 id_cocinero, pedido.id);
        imprimir_evento(mensaje);
    }

    char mensaje[80];
    snprintf(mensaje, sizeof(mensaje), "[COCINERO %d] Termina su trabajo.", id_cocinero);
    imprimir_evento(mensaje);

    marcar_cocinero_terminado();
    return NULL;
}

// threads repartidores
void *repartidor(void *arg)
{
    int id_repartidor = *(int *)arg;

    while (1)
    {
        Pedido pedido;
        char mensaje[180];
        int hay_pedido = 0;

        sem_wait(&pedidos_listos);
        pthread_mutex_lock(&mutex_cola_listos);

        if (cola_listos.cantidad > 0)
        {
            pedido = retirar_cola(&cola_listos);
            hay_pedido = 1;
        }

        pthread_mutex_unlock(&mutex_cola_listos);

        if (!hay_pedido)
        {
            if (todos_los_cocineros_terminaron())
            {
                break;
            }
            continue;
        }

        sem_post(&espacios_listos);

        snprintf(mensaje, sizeof(mensaje),
                 "[REPARTIDOR %d] Retira pedido listo -> ID: %d | Comida: %s",
                 id_repartidor, pedido.id, pedido.tipo_comida);
        imprimir_evento(mensaje);

        sleep(TIEMPO_ENTREGA);

        incrementar_pedidos_entregados();

        snprintf(mensaje, sizeof(mensaje),
                 "[REPARTIDOR %d] Pedido entregado -> ID: %d",
                 id_repartidor, pedido.id);
        imprimir_evento(mensaje);
    }

    char mensaje[80];
    snprintf(mensaje, sizeof(mensaje), "[REPARTIDOR %d] Termina su trabajo.", id_repartidor);
    imprimir_evento(mensaje);

    return NULL;
}

// inicializacion y limpieza
void inicializar_sincronizacion(void)
{
    pthread_mutex_init(&mutex_id, NULL);
    pthread_mutex_init(&mutex_generados, NULL);
    pthread_mutex_init(&mutex_entregados, NULL);
    pthread_mutex_init(&mutex_productores_terminados, NULL);
    pthread_mutex_init(&mutex_cocineros_terminados, NULL);
    pthread_mutex_init(&mutex_cola_pendientes, NULL);
    pthread_mutex_init(&mutex_cola_listos, NULL);
    pthread_mutex_init(&mutex_print, NULL);

    sem_init(&espacios_pendientes, 0, TAM_COLA_PENDIENTES);
    sem_init(&pedidos_pendientes, 0, 0);
    sem_init(&espacios_listos, 0, TAM_COLA_LISTOS);
    sem_init(&pedidos_listos, 0, 0);
}

void destruir_sincronizacion(void)
{
    sem_destroy(&espacios_pendientes);
    sem_destroy(&pedidos_pendientes);
    sem_destroy(&espacios_listos);
    sem_destroy(&pedidos_listos);

    pthread_mutex_destroy(&mutex_id);
    pthread_mutex_destroy(&mutex_generados);
    pthread_mutex_destroy(&mutex_entregados);
    pthread_mutex_destroy(&mutex_productores_terminados);
    pthread_mutex_destroy(&mutex_cocineros_terminados);
    pthread_mutex_destroy(&mutex_cola_pendientes);
    pthread_mutex_destroy(&mutex_cola_listos);
    pthread_mutex_destroy(&mutex_print);
}

// main
int main(void)
{
    pthread_t productores[NUM_PRODUCTORES];
    pthread_t cocineros[NUM_COCINEROS];
    pthread_t repartidores[NUM_REPARTIDORES];

    int ids_productores[NUM_PRODUCTORES];
    int ids_cocineros[NUM_COCINEROS];
    int ids_repartidores[NUM_REPARTIDORES];

    inicializar_cola(&cola_pendientes, TAM_COLA_PENDIENTES);
    inicializar_cola(&cola_listos, TAM_COLA_LISTOS);
    inicializar_sincronizacion();

    imprimir_evento("Sistema de Delivery iniciado. Generando pedidos...");

    for (int i = 0; i < NUM_PRODUCTORES; i++)
    {
        ids_productores[i] = i + 1;
        pthread_create(&productores[i], NULL, productor, &ids_productores[i]);
    }

    for (int i = 0; i < NUM_COCINEROS; i++)
    {
        ids_cocineros[i] = i + 1;
        pthread_create(&cocineros[i], NULL, cocinero, &ids_cocineros[i]);
    }

    for (int i = 0; i < NUM_REPARTIDORES; i++)
    {
        ids_repartidores[i] = i + 1;
        pthread_create(&repartidores[i], NULL, repartidor, &ids_repartidores[i]);
    }

    for (int i = 0; i < NUM_PRODUCTORES; i++)
    {
        pthread_join(productores[i], NULL);
    }

    for (int i = 0; i < NUM_COCINEROS; i++)
    {
        pthread_join(cocineros[i], NULL);
    }

    for (int i = 0; i < NUM_REPARTIDORES; i++)
    {
        pthread_join(repartidores[i], NULL);
    }

    char resumen[120];
    snprintf(resumen, sizeof(resumen),
             "Sistema finalizado | Pedidos generados: %d | Pedidos entregados: %d ",
             pedidos_generados, obtener_pedidos_entregados());
    imprimir_evento(resumen);

    destruir_sincronizacion();
    return 0;
}
