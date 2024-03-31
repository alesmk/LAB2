#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <pthread.h>
#include <sys/time.h>
#include <string.h>
#include <signal.h>

#include <sys/wait.h>
#include <sys/un.h> /* ind AF_UNIX */ /* struttura che rappresenta un indirizzo */

#include <util.h>
#include <boundedqueue.h>

#define MAX_LEN_FILENAME 255
#define TERM "-- FINE --"

#define UNIX_PATH_MAX 108
#define SOCKNAME "farm.sck"

sem_t *sem;
int checkTerm = 0;
volatile int fd_sk;

typedef struct fElem
{
    char *filename;
    long result;
} fElem_t;

typedef struct threadArgs
{
    int thid;
    BQueue_t *queue;
    struct sockaddr_un sa;

} threadArgs_t;

#define EC(sc, m)           \
    if (sc == -1)           \
    {                       \
        perror(m);          \
        exit(EXIT_FAILURE); \
    }

void handlerMW();
void handlerC();

void run_client(struct sockaddr_un *psa, fElem_t socketArg);
void run_server(struct sockaddr_un psa);

long getResult(long *arr, int N);
void *worker(void *arg);

int main(int argc, char *argv[])
{
    // LETTURA DEI PARAMETRI
    if (argc < 2)
    {
        perror("param");
        fprintf(stderr, "Errore num parametri\n");
        exit(EXIT_FAILURE);
    }

    struct sockaddr_un sa; /* ind AF_UNIX */

    sprintf(sa.sun_path, "%s", SOCKNAME);
    sa.sun_family = AF_UNIX;

    sem = sem_open("/sem", O_CREAT, 0644);

    if (sem == SEM_FAILED)
    {
        perror("Sem_open failed");
        exit(EXIT_FAILURE);
    }

    // 1: condiviso. 0: valore iniziale [V: +1 | P: -1]
    EC(sem_init(sem, 1, 0), "Sem_init failed");

    pid_t pid = fork();
    if (pid < 0)
    {
        perror("fork");
        exit(EXIT_FAILURE);
    }

    if (pid != 0) // PADRE, client socket
    {
        // installo dei handler SIGINT, SIGTSTP, SIGALRM, SIGTERM
        struct sigaction s;
        memset(&s, 0, sizeof(s));
        s.sa_handler = handlerMW;

        EC(sigaction(SIGHUP, &s, NULL), "sigaction SIGHUP");
        EC(sigaction(SIGINT, &s, NULL), "sigaction SIGINT");
        EC(sigaction(SIGQUIT, &s, NULL), "sigaction SIGQUIT");
        EC(sigaction(SIGTERM, &s, NULL), "sigaction SIGTERM");
        EC(sigaction(SIGPIPE, &s, NULL), "sigaction SIGPIPE");

        int n = 4, q = 8, t = 0, opt, f = 0;

        while ((opt = getopt(argc, argv, "n:q:t:")) != -1)
        {
            switch (opt)
            {
            case 'n':
                n = atoi(optarg);
                break;
            case 'q':
                q = atoi(optarg);
                break;
            case 't':
                t = atoi(optarg);
                break;
            default:
                break;
            }
        }

        pthread_t *workers = malloc(n * sizeof(pthread_t));
        threadArgs_t *thARGS = malloc(n * sizeof(threadArgs_t));
        fElem_t *fElem = malloc((argc - 1) * sizeof(threadArgs_t));

        if (!workers || !thARGS || !fElem)
        {
            fprintf(stderr, "not enough memory\n");
            exit(EXIT_FAILURE);
        }

        BQueue_t *queue = initBQueue(q);
        if (!queue)
        {
            fprintf(stderr, "initBQueue fallita\n");
            exit(errno);
        }

        P(sem); // aspetto che il socket venga creato

        for (int i = 0; i < n; i++)
        {
            thARGS[i].thid = i;
            thARGS[i].queue = queue;
            thARGS[i].sa = sa;

            if (pthread_create(&workers[i], NULL, worker, &thARGS[i]) != 0)
            {
                fprintf(stderr, "pthread_create failed\n");
                exit(EXIT_FAILURE);
            }
        }

        for (int index = optind; index < argc; index++)
        {
            if (checkTerm == 1)
                break;

            fElem[f].filename = argv[index];

            if (strlen(fElem[f].filename) > 255)
            {
                fprintf(stderr, "%s filename troppo lungo\n", fElem[f].filename);
                continue;
            }

            // CONTROLLO CHE IL FILE SIA REGOLARE
            size_t filesize;
            errno = 0;

            if (isRegular(fElem[f].filename, &filesize) != 1)
            {
                if (errno == 0)
                {
                    fprintf(stderr, "%s non e' un file regolare\n", fElem[f].filename);
                    continue;
                }
                perror("isRegular");
                continue;
            }

            EC(push(queue, &fElem[f]), "File push failed");

            f++;

            if (index != argc - 1) // se è l'ultimo, non aspetto
                usleep(t * 1000);
        }

        fElem[f].filename = TERM;
        EC(push(queue, &fElem[f]), "TERM push failed");

        checkTerm = 1;

        for (int i = 0; i < n; i++)
            EC(pthread_join(workers[i], NULL), "pthread_join failed");

       
        fElem_t socketArg;
        socketArg.filename = TERM;
        socketArg.result = 0;
        run_client(&sa, socketArg);

        free(workers);
        free(fElem);
        free(thARGS);

        deleteBQueue(queue, NULL);

        EC(sem_close(sem), "Sem_close failed");
        EC(sem_destroy(sem), "sem_destroy failed");
        EC(sem_unlink("/sem"), "Sem_unlink failed");
    }

    // COLLECTOR: figlio, server socket in attesa
    else
    {
        V(sem);

        // maschero tutti i segnali gestiti da worker
        struct sigaction s;
        memset(&s, 0, sizeof(s));

        sigset_t set;
        s.sa_flags = 0;
        s.sa_handler = handlerC;

        EC(sigemptyset(&set), "emptyset");
        EC(pthread_sigmask(SIG_SETMASK, &set, NULL), "sigmask");

        EC(sigaddset(&set, SIGHUP), "sigaddset SIGHUP");
        EC(sigaddset(&set, SIGINT), "sigaddset SIGINT");
        EC(sigaddset(&set, SIGQUIT), "sigaddset SIGQUIT");
        EC(sigaddset(&set, SIGTERM), "sigaddset SIGTERM");

        EC(sigaction(SIGPIPE, &s, NULL), "sigaction SIGPIPE");

        EC(pthread_sigmask(SIG_SETMASK, &set, NULL), "sigmask");

        run_server(sa); // readFromWorker

        EC(sem_close(sem), "Sem_close collector failed");

        exit(EXIT_SUCCESS);
    }

    EC(waitpid(pid, NULL, 0), "waitpid");

    remove(SOCKNAME);

    return 0;
}

/* HANDLER */
void handlerMW()
{
    checkTerm = 1;
}

void handlerC()
{
    checkTerm = 1;
    shutdown(fd_sk, SHUT_RD);
}

/* ELABORAZIONE */
long getResult(long *arr, int N)
{
    long res = 0;

    for (int i = 0; i < N; i++)
        res += (arr[i] * i);

    return res;
}

void *worker(void *arg)
{
    BQueue_t *queue = ((threadArgs_t *)arg)->queue;
    struct sockaddr_un sa = ((threadArgs_t *)arg)->sa;

    int N;
    char pathname[1024];

    // struct necessaria per stat
    struct stat fstatbuf;

    while (1)
    {
        fElem_t *fElem = pop(queue);
        char *filename = fElem->filename;

        if (strcmp(filename, TERM) == 0)
        {
            EC(push(queue, fElem), "TERM push failed");
            return NULL;
        }

        // calcolo quanti long contiene il file
        char dirnamebuf[1024 - 255];
        if (getcwd(dirnamebuf, 1024 - 255) == NULL)
        {
            perror("getcwd");
            exit(EXIT_FAILURE);
        }

        sprintf(pathname, "%s/%s", dirnamebuf, filename);

        if (stat(pathname, &fstatbuf) == -1)
        {
            perror("stat");
            fprintf(stderr, "Errore facendo stat di %s\n", pathname);
        }

        N = fstatbuf.st_size / 8;

        long *num = malloc(N * sizeof(long));
        if (!num)
        {
            fprintf(stderr, "not enough memory, num\n");
            exit(EXIT_FAILURE);
        }

        // apro e leggo il file
        FILE *file = fopen(filename, "rb");
        if (file == NULL)
        {
            fprintf(stderr, "Errore apertura file %s\n", filename);
            return NULL;
        }

        int r;
        for (int i = 0; i < N; i++)
        {
            r = fread(&num[i], sizeof(long), 1, file);
            if (r < 0)
            {
                fprintf(stderr, "Errore lettura file %s\n", filename);
                return NULL;
            }
        }

        // TRAMITE IL SOCKET INVIO IL RISULTATO A COLLECTOR, CON LA CONNESSIONE STABILITA PRECEDENTEMENTE
        fElem_t socketArg;
        socketArg.filename = filename;
        socketArg.result = getResult(num, N);

        fclose(file);
        free(num);

        run_client(&sa, socketArg);
    }
    return NULL;
}

/* SOCKET */
void run_server(struct sockaddr_un psa)
{

    int fd_c;
    int nread;
    fElem_t *socketArg;

    remove(SOCKNAME);

    EC((fd_sk = socket(AF_UNIX, SOCK_STREAM, 0)), "socket run_server error");
    EC(bind(fd_sk, (struct sockaddr *)&psa, sizeof(psa)), "bind error");
    EC(listen(fd_sk, SOMAXCONN), "listen error");

    V(sem); // il socket è pronto

    while (checkTerm == 0)
    {

        fd_c = accept(fd_sk, NULL, NULL);

        if (fd_c < 0)
        {
            if (checkTerm == 1)
                break;
            continue;
        }

        socketArg = malloc(sizeof(fElem_t));
        if (!socketArg)
        {
            fprintf(stderr, "not enough memory\n");
            exit(EXIT_FAILURE);
        }

        nread = read(fd_c, socketArg, sizeof(fElem_t));
        if (nread < 0)
        {
            perror("Read error");
            free(socketArg);
            break;
        }

        if (nread > 0)
        {
            if(strcmp(socketArg->filename, TERM) == 0){
                close(fd_c);
                free(socketArg);
                break;
            }
            printf("%lu %s\n", socketArg->result, socketArg->filename); // output -> result filename
        }
        close(fd_c);
        free(socketArg);
    }
    close(fd_sk);

} /* chiude run_server */

void run_client(struct sockaddr_un *psa, fElem_t socketArg)
{
    int fd_skt;
    EC((fd_skt = socket(AF_UNIX, SOCK_STREAM, 0)), "socket run_client error");

    while (connect(fd_skt, (struct sockaddr *)psa, sizeof(*psa)) == -1)
    {
        if (errno == ENOENT)
            sleep(1);
        else
            exit(EXIT_FAILURE);
    }

    int res = write(fd_skt, &socketArg, sizeof(fElem_t));

    if (res < 0)
        perror("Errore write");

    close(fd_skt);
}