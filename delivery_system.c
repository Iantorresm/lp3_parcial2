/*
 * Sistema concurrente de delivery con Pthreads.
 *
 * Mapa rápido para skimming:
 * 1. Productores generan pedidos y los insertan en cola_pendientes.
 * 2. Cocineros consumen cola_pendientes, preparan y pasan a cola_listos.
 * 3. Repartidores consumen cola_listos y marcan pedidos como entregados.
 * 4. main coordina el ciclo de vida: inicializa recursos, crea threads,
 *    espera productores, envía señales de fin, espera consumidores y libera todo.
 *
 * Idea central:
 * hay dos colas protegidas por mutex + semáforos. El mutex protege los datos
 * internos de cada cola; los semáforos evitan busy waiting cuando una cola está
 * llena o vacía.
 */

#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <string.h>

/* Configuración principal del escenario concurrente. */
#define NUM_PRODUCTORES 3
#define NUM_COCINEROS 2
#define NUM_REPARTIDORES 2

/* Cada productor genera una cantidad finita para que pthread_join pueda terminar. */
#define PEDIDOS_POR_PRODUCTOR 4

/* Capacidades máximas de las colas compartidas. */
#define TAM_COLA_PENDIENTES 5
#define TAM_COLA_LISTOS 5
#define TAM_MAX_COLA 20

#define TOTAL_PEDIDOS (NUM_PRODUCTORES * PEDIDOS_POR_PRODUCTOR)

/* Pedido: unidad de trabajo que viaja por el pipeline completo. */
typedef struct
{
    int id;
    char tipo_comida[30];
    int tiempo_preparacion;
} Pedido;

/*
 * Cola circular simple.
 * frente apunta al próximo elemento a sacar; final apunta al próximo lugar libre.
 */
typedef struct
{
    Pedido pedidos[TAM_MAX_COLA];
    int frente;
    int final;
    int cantidad;
} Cola;

/* Etapa 1: pedidos generados pero todavía no preparados. */
Cola cola_pendientes;

/* Etapa 2: pedidos preparados, esperando repartidor. */
Cola cola_listos;

/* Estado global compartido entre threads. */
int siguiente_id = 1;
int pedidos_entregados = 0;

/*
 * Mutexes:
 * - mutex_id evita que dos productores asignen el mismo ID.
 * - mutex_pendientes protege cola_pendientes.
 * - mutex_listos protege cola_listos.
 * - mutex_entregados protege el contador final.
 */
pthread_mutex_t mutex_id;
pthread_mutex_t mutex_pendientes;
pthread_mutex_t mutex_listos;
pthread_mutex_t mutex_entregados;

/*
 * Semáforos de productor/consumidor:
 * - espacios_* cuenta lugares libres en cada cola.
 * - pedidos_* cuenta pedidos disponibles para consumir.
 */
sem_t espacios_pendientes;
sem_t pedidos_pendientes;
sem_t espacios_listos;
sem_t pedidos_listos;

/* Deja una cola lista para usarse desde cero. */
void inicializar_cola(Cola *cola)
{
    cola->frente = 0;
    cola->final = 0;
    cola->cantidad = 0;
}

/* Inserta en cola circular. La sincronización ocurre fuera de esta función. */
void insertar_pedido(Cola *cola, Pedido pedido, int tam_cola)
{
    cola->pedidos[cola->final] = pedido;
    cola->final = (cola->final + 1) % tam_cola;
    cola->cantidad++;
}

/* Saca de cola circular. La sincronización ocurre fuera de esta función. */
Pedido sacar_pedido(Cola *cola, int tam_cola)
{
    Pedido pedido = cola->pedidos[cola->frente];
    cola->frente = (cola->frente + 1) % tam_cola;
    cola->cantidad--;
    return pedido;
}

/*
 * Entrada a la primera etapa.
 * Si la cola está llena, sem_wait bloquea al productor sin gastar CPU.
 */
void insertar_en_pendientes(Pedido pedido)
{
    sem_wait(&espacios_pendientes);

    pthread_mutex_lock(&mutex_pendientes);
    insertar_pedido(&cola_pendientes, pedido, TAM_COLA_PENDIENTES);

    if (pedido.id != -1)
    {
        printf("[COLA PENDIENTES] Pedido ID %d insertado | Cantidad pendientes: %d\n",
               pedido.id, cola_pendientes.cantidad);
    }

    pthread_mutex_unlock(&mutex_pendientes);

    sem_post(&pedidos_pendientes);
}

/*
 * Entrada a la segunda etapa.
 * Los cocineros publican acá los pedidos que ya pueden ser retirados.
 */
void insertar_en_listos(Pedido pedido)
{
    sem_wait(&espacios_listos);

    pthread_mutex_lock(&mutex_listos);
    insertar_pedido(&cola_listos, pedido, TAM_COLA_LISTOS);

    if (pedido.id != -1)
    {
        printf("[COLA LISTOS] Pedido ID %d listo para entrega | Cantidad listos: %d\n",
               pedido.id, cola_listos.cantidad);
    }

    pthread_mutex_unlock(&mutex_listos);

    sem_post(&pedidos_listos);
}

/*
 * Productor:
 * genera pedidos reales, asigna ID único y los encola como pendientes.
 */
void *productor(void *arg)
{
    int id_productor = *(int *)arg;

    char comidas[5][30] = {
        "Pizza",
        "Hamburguesa",
        "Milanesa",
        "Lomito",
        "Empanada"};

    for (int i = 0; i < PEDIDOS_POR_PRODUCTOR; i++)
    {
        Pedido pedido;

        /* Zona crítica: el ID global debe avanzar de a un thread por vez. */
        pthread_mutex_lock(&mutex_id);
        pedido.id = siguiente_id;
        siguiente_id++;
        pthread_mutex_unlock(&mutex_id);

        pedido.tiempo_preparacion = (pedido.id % 3) + 1;
        strcpy(pedido.tipo_comida, comidas[pedido.id % 5]);

        printf("[PRODUCTOR %d] Pedido generado -> ID: %d | Comida: %s | Preparacion: %d seg\n",
               id_productor, pedido.id, pedido.tipo_comida, pedido.tiempo_preparacion);

        insertar_en_pendientes(pedido);

        sleep(1);
    }

    return NULL;
}

/*
 * Cocinero:
 * consume exclusivamente de cola_pendientes y produce exclusivamente en cola_listos.
 */
void *cocinero(void *arg)
{
    int id_cocinero = *(int *)arg;

    while (1)
    {
        /* Espera hasta que exista al menos un pedido pendiente. */
        sem_wait(&pedidos_pendientes);

        pthread_mutex_lock(&mutex_pendientes);
        Pedido pedido = sacar_pedido(&cola_pendientes, TAM_COLA_PENDIENTES);

        printf("[COCINERO %d] Pedido tomado -> ID: %d | Pendientes: %d\n",
               id_cocinero, pedido.id, cola_pendientes.cantidad);

        pthread_mutex_unlock(&mutex_pendientes);

        sem_post(&espacios_pendientes);

        /*
         * Pedido centinela:
         * no representa comida; es una señal para cerrar el thread sin matarlo.
         */
        if (pedido.id == -1)
        {
            printf("[COCINERO %d] Recibio señal de fin. Termina.\n", id_cocinero);
            break;
        }

        printf("[COCINERO %d] Preparando pedido ID %d | Comida: %s\n",
               id_cocinero, pedido.id, pedido.tipo_comida);

        sleep(pedido.tiempo_preparacion);

        printf("[COCINERO %d] Pedido listo -> ID: %d\n", id_cocinero, pedido.id);

        insertar_en_listos(pedido);
    }

    return NULL;
}

/*
 * Repartidor:
 * consume exclusivamente de cola_listos. Nunca mira cola_pendientes.
 */
void *repartidor(void *arg)
{
    int id_repartidor = *(int *)arg;

    while (1)
    {
        /* Espera hasta que exista al menos un pedido listo para entregar. */
        sem_wait(&pedidos_listos);

        pthread_mutex_lock(&mutex_listos);
        Pedido pedido = sacar_pedido(&cola_listos, TAM_COLA_LISTOS);

        printf("[REPARTIDOR %d] Retira pedido listo -> ID: %d | Listos: %d\n",
               id_repartidor, pedido.id, cola_listos.cantidad);

        pthread_mutex_unlock(&mutex_listos);

        sem_post(&espacios_listos);

        /* Centinela propio de repartidores: llega por cola_listos. */
        if (pedido.id == -1)
        {
            printf("[REPARTIDOR %d] Recibio señal de fin. Termina.\n", id_repartidor);
            break;
        }

        printf("[REPARTIDOR %d] Entregando pedido ID %d | Comida: %s\n",
               id_repartidor, pedido.id, pedido.tipo_comida);

        sleep(1);

        pthread_mutex_lock(&mutex_entregados);
        pedidos_entregados++;

        printf("[REPARTIDOR %d] Pedido entregado -> ID: %d | Total entregados: %d\n",
               id_repartidor, pedido.id, pedidos_entregados);

        pthread_mutex_unlock(&mutex_entregados);
    }

    return NULL;
}

int main()
{
    pthread_t productores[NUM_PRODUCTORES];
    pthread_t cocineros[NUM_COCINEROS];
    pthread_t repartidores[NUM_REPARTIDORES];

    int ids_productores[NUM_PRODUCTORES];
    int ids_cocineros[NUM_COCINEROS];
    int ids_repartidores[NUM_REPARTIDORES];

    /* Fase 1: preparar estructuras compartidas antes de crear threads. */
    inicializar_cola(&cola_pendientes);
    inicializar_cola(&cola_listos);

    pthread_mutex_init(&mutex_id, NULL);
    pthread_mutex_init(&mutex_pendientes, NULL);
    pthread_mutex_init(&mutex_listos, NULL);
    pthread_mutex_init(&mutex_entregados, NULL);

    sem_init(&espacios_pendientes, 0, TAM_COLA_PENDIENTES);
    sem_init(&pedidos_pendientes, 0, 0);
    sem_init(&espacios_listos, 0, TAM_COLA_LISTOS);
    sem_init(&pedidos_listos, 0, 0);

    printf("=== INICIO DEL SISTEMA DELIVERY ===\n\n");

    /*
     * Fase 2: crear consumidores antes que productores.
     * Así ya quedan bloqueados esperando trabajo en sus semáforos.
     */
    for (int i = 0; i < NUM_REPARTIDORES; i++)
    {
        ids_repartidores[i] = i + 1;
        pthread_create(&repartidores[i], NULL, repartidor, &ids_repartidores[i]);
    }

    for (int i = 0; i < NUM_COCINEROS; i++)
    {
        ids_cocineros[i] = i + 1;
        pthread_create(&cocineros[i], NULL, cocinero, &ids_cocineros[i]);
    }

    for (int i = 0; i < NUM_PRODUCTORES; i++)
    {
        ids_productores[i] = i + 1;
        pthread_create(&productores[i], NULL, productor, &ids_productores[i]);
    }

    /* Fase 3: esperar a que termine toda la generación de pedidos. */
    for (int i = 0; i < NUM_PRODUCTORES; i++)
    {
        pthread_join(productores[i], NULL);
    }

    /*
     * Fase 4: cerrar la etapa de cocina.
     * Se envía un centinela por cocinero para que todos puedan salir del while.
     */
    for (int i = 0; i < NUM_COCINEROS; i++)
    {
        Pedido fin_cocinero;
        fin_cocinero.id = -1;
        strcpy(fin_cocinero.tipo_comida, "FIN");
        fin_cocinero.tiempo_preparacion = 0;

        insertar_en_pendientes(fin_cocinero);
    }

    for (int i = 0; i < NUM_COCINEROS; i++)
    {
        pthread_join(cocineros[i], NULL);
    }

    /*
     * Fase 5: cerrar la etapa de reparto.
     * Recién ahora se sabe que ningún cocinero agregará más pedidos listos.
     */
    for (int i = 0; i < NUM_REPARTIDORES; i++)
    {
        Pedido fin_repartidor;
        fin_repartidor.id = -1;
        strcpy(fin_repartidor.tipo_comida, "FIN");
        fin_repartidor.tiempo_preparacion = 0;

        insertar_en_listos(fin_repartidor);
    }

    for (int i = 0; i < NUM_REPARTIDORES; i++)
    {
        pthread_join(repartidores[i], NULL);
    }

    printf("\n=== RESUMEN FINAL ===\n");
    printf("Pedidos esperados: %d\n", TOTAL_PEDIDOS);
    printf("Pedidos entregados: %d\n", pedidos_entregados);

    /* Fase 6: liberar recursos POSIX creados con sem_init/pthread_mutex_init. */
    sem_destroy(&espacios_pendientes);
    sem_destroy(&pedidos_pendientes);
    sem_destroy(&espacios_listos);
    sem_destroy(&pedidos_listos);

    pthread_mutex_destroy(&mutex_id);
    pthread_mutex_destroy(&mutex_pendientes);
    pthread_mutex_destroy(&mutex_listos);
    pthread_mutex_destroy(&mutex_entregados);

    printf("\n=== FIN DEL SISTEMA DELIVERY ===\n");

    return 0;
}
