

#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <string.h>

#define NUM_PRODUCTORES 3
#define NUM_COCINEROS 2
#define NUM_REPARTIDORES 2

#define PEDIDOS_POR_PRODUCTOR 4

#define TAM_COLA_PENDIENTES 5
#define TAM_COLA_LISTOS 5
#define TAM_MAX_COLA 20

#define TOTAL_PEDIDOS (NUM_PRODUCTORES * PEDIDOS_POR_PRODUCTOR)

typedef struct {
    int id;
    char tipo_comida[30];
    int tiempo_preparacion;
} Pedido;

typedef struct {
    Pedido pedidos[TAM_MAX_COLA];
    int frente;
    int final;
    int cantidad;
} Cola;

Cola cola_pendientes;
Cola cola_listos;

int siguiente_id = 1;
int pedidos_entregados = 0;

pthread_mutex_t mutex_id;
pthread_mutex_t mutex_pendientes;
pthread_mutex_t mutex_listos;
pthread_mutex_t mutex_entregados;

sem_t espacios_pendientes;
sem_t pedidos_pendientes;
sem_t espacios_listos;
sem_t pedidos_listos;

void inicializar_cola(Cola *cola)
{
    cola->frente = 0;
    cola->final = 0;
    cola->cantidad = 0;
}

void insertar_pedido(Cola *cola, Pedido pedido, int tam_cola)
{
    cola->pedidos[cola->final] = pedido;
    cola->final = (cola->final + 1) % tam_cola;
    cola->cantidad++;
}

Pedido sacar_pedido(Cola *cola, int tam_cola)
{
    Pedido pedido = cola->pedidos[cola->frente];
    cola->frente = (cola->frente + 1) % tam_cola;
    cola->cantidad--;
    return pedido;
}

void insertar_en_pendientes(Pedido pedido)
{
    sem_wait(&espacios_pendientes);

    pthread_mutex_lock(&mutex_pendientes);
    insertar_pedido(&cola_pendientes, pedido, TAM_COLA_PENDIENTES);

    if (pedido.id != -1) {
        printf("[COLA PENDIENTES] Pedido ID %d insertado | Cantidad pendientes: %d\n",
               pedido.id, cola_pendientes.cantidad);
    }

    pthread_mutex_unlock(&mutex_pendientes);

    sem_post(&pedidos_pendientes);
}

void insertar_en_listos(Pedido pedido)
{
    sem_wait(&espacios_listos);

    pthread_mutex_lock(&mutex_listos);
    insertar_pedido(&cola_listos, pedido, TAM_COLA_LISTOS);

    if (pedido.id != -1) {
        printf("[COLA LISTOS] Pedido ID %d listo para entrega | Cantidad listos: %d\n",
               pedido.id, cola_listos.cantidad);
    }

    pthread_mutex_unlock(&mutex_listos);

    sem_post(&pedidos_listos);
}

void *productor(void *arg)
{
    int id_productor = *(int *)arg;

    char comidas[5][30] = {
        "Pizza",
        "Hamburguesa",
        "Milanesa",
        "Lomito",
        "Empanada"
    };

    for (int i = 0; i < PEDIDOS_POR_PRODUCTOR; i++) {
        Pedido pedido;

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

void *cocinero(void *arg)
{
    int id_cocinero = *(int *)arg;

    while (1) {
        sem_wait(&pedidos_pendientes);

        pthread_mutex_lock(&mutex_pendientes);
        Pedido pedido = sacar_pedido(&cola_pendientes, TAM_COLA_PENDIENTES);

        printf("[COCINERO %d] Pedido tomado -> ID: %d | Pendientes: %d\n",
               id_cocinero, pedido.id, cola_pendientes.cantidad);

        pthread_mutex_unlock(&mutex_pendientes);

        sem_post(&espacios_pendientes);

        if (pedido.id == -1) {
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

void *repartidor(void *arg)
{
    int id_repartidor = *(int *)arg;

    while (1) {
        sem_wait(&pedidos_listos);

        pthread_mutex_lock(&mutex_listos);
        Pedido pedido = sacar_pedido(&cola_listos, TAM_COLA_LISTOS);

        printf("[REPARTIDOR %d] Retira pedido listo -> ID: %d | Listos: %d\n",
               id_repartidor, pedido.id, cola_listos.cantidad);

        pthread_mutex_unlock(&mutex_listos);

        sem_post(&espacios_listos);

        if (pedido.id == -1) {
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

    for (int i = 0; i < NUM_REPARTIDORES; i++) {
        ids_repartidores[i] = i + 1;
        pthread_create(&repartidores[i], NULL, repartidor, &ids_repartidores[i]);
    }

    for (int i = 0; i < NUM_COCINEROS; i++) {
        ids_cocineros[i] = i + 1;
        pthread_create(&cocineros[i], NULL, cocinero, &ids_cocineros[i]);
    }

    for (int i = 0; i < NUM_PRODUCTORES; i++) {
        ids_productores[i] = i + 1;
        pthread_create(&productores[i], NULL, productor, &ids_productores[i]);
    }

    for (int i = 0; i < NUM_PRODUCTORES; i++) {
        pthread_join(productores[i], NULL);
    }

    for (int i = 0; i < NUM_COCINEROS; i++) {
        Pedido fin_cocinero;
        fin_cocinero.id = -1;
        strcpy(fin_cocinero.tipo_comida, "FIN");
        fin_cocinero.tiempo_preparacion = 0;

        insertar_en_pendientes(fin_cocinero);
    }

    for (int i = 0; i < NUM_COCINEROS; i++) {
        pthread_join(cocineros[i], NULL);
    }

    for (int i = 0; i < NUM_REPARTIDORES; i++) {
        Pedido fin_repartidor;
        fin_repartidor.id = -1;
        strcpy(fin_repartidor.tipo_comida, "FIN");
        fin_repartidor.tiempo_preparacion = 0;

        insertar_en_listos(fin_repartidor);
    }

    for (int i = 0; i < NUM_REPARTIDORES; i++) {
        pthread_join(repartidores[i], NULL);
    }

    printf("\n=== RESUMEN FINAL ===\n");
    printf("Pedidos esperados: %d\n", TOTAL_PEDIDOS);
    printf("Pedidos entregados: %d\n", pedidos_entregados);

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
