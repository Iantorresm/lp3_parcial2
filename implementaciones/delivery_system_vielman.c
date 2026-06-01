#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <string.h>
#include <semaphore.h>
#include <pthread.h> // CORRECCIÓN: Sin 's' al final
#include <stdint.h>  // CORRECCIÓN: Para castear (intptr_t) de forma segura

pthread_mutex_t mutex_pendiente, mutex_listos;
sem_t pendiente_libres, pendiente_agregados;
sem_t listos_libres, listos_agregados;

typedef struct pedido
{
    int id;
    int tiempo_estimado;
    char tipo_comida[50];
    struct pedido *siguiente; // CORRECCIÓN: Faltaba la palabra "struct"
} pedido;

typedef struct cola_pedidos
{
    pedido *primer_pedido;
    pedido *ultimo_pedido;
} cola_pedidos;

int cola_esta_vacia(cola_pedidos *q)
{
    if (q->primer_pedido == NULL)
    {
        return 1;
    } // CORRECCIÓN: '==' en lugar de '='
    return 0;
}

void encolar_pedido(cola_pedidos *q, pedido *p)
{
    if (q->primer_pedido == NULL)
    {
        q->primer_pedido = p;
        q->ultimo_pedido = p;
    }
    else
    {
        q->ultimo_pedido->siguiente = p;
        q->ultimo_pedido = p;
    }
}

pedido *desencolar_pedido(cola_pedidos *q)
{
    if (q->primer_pedido == NULL)
    { // CORRECCIÓN: '==' en lugar de '='
        return NULL;
    }

    // CORRECCIÓN: Comparar punteros directamente, no sus direcciones de memoria
    if (q->primer_pedido == q->ultimo_pedido)
    {
        pedido *p = q->primer_pedido;
        q->primer_pedido = NULL;
        q->ultimo_pedido = NULL;
        return p;
    }

    pedido *p = q->primer_pedido;
    q->primer_pedido = q->primer_pedido->siguiente;
    return p;
}

const char *tipos_comida[] = {"Carnes", "Pastas", "Postres", "Picadas"}; // CORRECCIÓN: Arreglo de punteros
cola_pedidos *pedidos_pendientes;
cola_pedidos *pedidos_listos;
int id_pedidos = 0;
pthread_mutex_t mutex_id;

void *generar_pedido(void *nro_prod)
{
    // CORRECCIÓN: Casteo seguro del argumento
    int productor = (int)(intptr_t)nro_prod;
    int cant_pedidos = rand() % 3 + 1;

    for (int i = 0; i < cant_pedidos; i++)
    {
        pedido *nuevo_pedido = (pedido *)malloc(sizeof(pedido));

        // generamos un id al pedido de forma segura con su mutex
        pthread_mutex_lock(&mutex_id);
        int id_ped = id_pedidos;
        nuevo_pedido->id = id_ped;
        id_pedidos++;
        pthread_mutex_unlock(&mutex_id);

        nuevo_pedido->tiempo_estimado = rand() % 5 + 1;
        int comida_tipo = rand() % 4;
        strcpy(nuevo_pedido->tipo_comida, tipos_comida[comida_tipo]); // CORRECCIÓN: Asignación correcta de strings
        nuevo_pedido->siguiente = NULL;

        // Esperamos a que la cola de pendiente tenga espacio y agregamos
        sem_wait(&pendiente_libres);
        pthread_mutex_lock(&mutex_pendiente);

        printf("[PRODUCTOR : %d] : agrego un pedido a la cola pedidos_pendientes\n", productor);
        printf("Pedido id: %d,\ntipo de comida: %s,\ntiempo estimado de preparacion: %d segundos\n\n", nuevo_pedido->id, nuevo_pedido->tipo_comida, nuevo_pedido->tiempo_estimado);

        encolar_pedido(pedidos_pendientes, nuevo_pedido);
        pthread_mutex_unlock(&mutex_pendiente);
        sem_post(&pendiente_agregados);

        int descanso = rand() % 3 + 1;
        sleep(descanso); // CORRECCIÓN: Faltaba punto y coma
    }

    printf("[PRODUCTOR : %d] : se retira...\n", productor);
    return NULL;
}

void *cocinar_pedido(void *nro_coci)
{
    int nro_cocinero = (int)(intptr_t)nro_coci;

    while (1)
    {
        // agarramos un pedido de los pendientes y lo cocinamos
        sem_wait(&pendiente_agregados);
        pthread_mutex_lock(&mutex_pendiente);
        pedido *p = desencolar_pedido(pedidos_pendientes);
        pthread_mutex_unlock(&mutex_pendiente);
        sem_post(&pendiente_libres);
        // Si es veneno el cocinero termina
        if (p->id == -1)
        {
            printf("[COCINERO : %d] : se retira...\n", nro_cocinero);
            free(p); // Limpiamos la memoria del veneno
            break;
        }

        printf("[COCINERO : %d] : se encuentra preparando el pedido: %d\n", nro_cocinero, p->id);
        int tiempo_cocinando = p->tiempo_estimado;
        sleep(tiempo_cocinando); // CORRECCIÓN: Variable correcta

        // mandamos el pedido ya cocinado en la cola de listos para enviar
        sem_wait(&listos_libres);
        pthread_mutex_lock(&mutex_listos);
        encolar_pedido(pedidos_listos, p);
        printf("[COCINERO : %d] : termino de cocinar el pedido: %d y lo agrego a la cola de pedidos listos\n", nro_cocinero, p->id);
        pthread_mutex_unlock(&mutex_listos);
        sem_post(&listos_agregados);
    }

    return NULL;
}

void *repartir_pedido(void *nro_rep)
{
    int nro_repartidor = (int)(intptr_t)nro_rep; // CORRECCIÓN: Tipo int, no 'repartidor'

    while (1)
    {
        // Esperamos a que haya un pedido en la lista de listos para enviar
        sem_wait(&listos_agregados);
        pthread_mutex_lock(&mutex_listos);
        pedido *p = desencolar_pedido(pedidos_listos);

        // CORRECCIÓN: El unlock debe ir ANTES del break para no causar Deadlock
        pthread_mutex_unlock(&mutex_listos);
        sem_post(&listos_libres);
        if (p->id == -1)
        {
            printf("[REPARTIDOR : %d] : se retira...\n", nro_repartidor);
            free(p); // Limpiamos la memoria del veneno
            break;
        }

        // CORRECCIÓN: Simular entrega con sleep (Requisito del PDF)
        int tiempo_entrega = rand() % 3 + 1;
        sleep(tiempo_entrega);

        printf("[REPARTIDOR : %d] : entrego el pedido: %d\n\n", nro_repartidor, p->id);
        free(p);
    }
    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc != 5)
    {
        printf("Cantidad de argumentos invalida, por favor introduce:\n");
        printf("./programa <tam_cola> <productores> <cocineros> <repartidores>\n");
        return 1; // CORRECCIÓN: Terminar el programa si faltan argumentos
    }

    srand(time(NULL));

    int max_cola = atoi(argv[1]);
    int cant_productores = atoi(argv[2]);
    int cant_cocineros = atoi(argv[3]);
    int cant_repartidores = atoi(argv[4]);

    pedidos_pendientes = (cola_pedidos *)malloc(sizeof(cola_pedidos));
    pedidos_pendientes->primer_pedido = NULL;
    pedidos_pendientes->ultimo_pedido = NULL;

    pedidos_listos = (cola_pedidos *)malloc(sizeof(cola_pedidos));
    pedidos_listos->primer_pedido = NULL;
    pedidos_listos->ultimo_pedido = NULL;

    pthread_t *productores = malloc(cant_productores * sizeof(pthread_t));
    pthread_t *cocineros = malloc(cant_cocineros * sizeof(pthread_t));
    pthread_t *repartidores = malloc(cant_repartidores * sizeof(pthread_t));

    // CORRECCIÓN: Inicialización de Mutex
    pthread_mutex_init(&mutex_pendiente, NULL);
    pthread_mutex_init(&mutex_listos, NULL);
    pthread_mutex_init(&mutex_id, NULL);

    sem_init(&pendiente_libres, 0, max_cola);
    sem_init(&pendiente_agregados, 0, 0);
    sem_init(&listos_libres, 0, max_cola);
    sem_init(&listos_agregados, 0, 0);

    // creamos los hilos
    //  CORRECCIÓN: Pasar el valor casteado a puntero para evitar Race Condition
    for (int i = 0; i < cant_productores; i++)
    {
        pthread_create(&productores[i], NULL, generar_pedido, (void *)(intptr_t)i);
    }
    for (int i = 0; i < cant_cocineros; i++)
    {
        pthread_create(&cocineros[i], NULL, cocinar_pedido, (void *)(intptr_t)i);
    }
    for (int i = 0; i < cant_repartidores; i++)
    {
        pthread_create(&repartidores[i], NULL, repartir_pedido, (void *)(intptr_t)i); // CORRECCIÓN: Era repartidores[i]
    }

    // bloqueamos al main esperando los hilos productores
    for (int i = 0; i < cant_productores; i++)
    {
        pthread_join(productores[i], NULL);
    }

    // Avisamos a los cocineros para que se retiren
    for (int i = 0; i < cant_cocineros; i++)
    {
        // CORRECCIÓN: El malloc del veneno debe ir dentro del for
        pedido *veneno = malloc(sizeof(pedido));
        veneno->id = -1;
        veneno->siguiente = NULL;

        sem_wait(&pendiente_libres); // Respetar el límite de la cola
        pthread_mutex_lock(&mutex_pendiente);
        encolar_pedido(pedidos_pendientes, veneno);
        pthread_mutex_unlock(&mutex_pendiente);
        sem_post(&pendiente_agregados);
    }

    for (int i = 0; i < cant_cocineros; i++)
    {
        pthread_join(cocineros[i], NULL);
    }

    // Avisamos a los repartidores para que se retiren
    for (int i = 0; i < cant_repartidores; i++)
    {
        pedido *veneno = malloc(sizeof(pedido)); // CORRECCIÓN: Un nodo único por repartidor
        veneno->id = -1;
        veneno->siguiente = NULL;

        sem_wait(&listos_libres);
        pthread_mutex_lock(&mutex_listos);
        encolar_pedido(pedidos_listos, veneno);
        pthread_mutex_unlock(&mutex_listos);
        sem_post(&listos_agregados);
    }

    for (int i = 0; i < cant_repartidores; i++)
    {
        pthread_join(repartidores[i], NULL);
    }

    // CORRECCIÓN: Destruir semáforos y mutexes según requisitos obligatorios
    pthread_mutex_destroy(&mutex_pendiente);
    pthread_mutex_destroy(&mutex_listos);
    pthread_mutex_destroy(&mutex_id);

    sem_destroy(&pendiente_libres);
    sem_destroy(&pendiente_agregados);
    sem_destroy(&listos_libres);
    sem_destroy(&listos_agregados);

    free(productores);
    free(cocineros);
    free(repartidores);
    free(pedidos_pendientes);
    free(pedidos_listos);

    printf("Cerrando el Sistema de Pedidos con Exito!!!\n");
    return 0;
}
