/* Web proxy
 * This is a web proxy that passes client's request
 * to server and passes server's response back to 
 * client. It is able to handle multiple requests in
 * parallel and has a cache. In order to prevent 
 * race conditions, this web proxy ensures that when
 * a thread is reading the cache, no other threads are
 * modifying it; if a thread is writing to the cache,
 * no other threads are reading or writing to it. */


#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "csapp.h"

#define MAX_CACHE_SIZE (1 << 20)
#define MAX_OBJECT_SIZE (100 * (1 << 10))


/* client_request is a structure that holds 
 * request from client */
struct client_request
{
	char request_line[MAXLINE];
	char request[MAXBUF];
	char host[MAXLINE];
	int server_port;
	int request_length;
};

/* cache_cell is a structure that holds
 * information of a cache cell */
struct cache_cell
{
	char request_line[MAXLINE];
	void *content;
	int size;
	unsigned long last_use;
	struct cache_cell *previous;
	struct cache_cell *next;
	pthread_rwlock_t lock;    /* each cache cell has a rwlock */
};



/* function declarations */
struct client_request *allocate_request();
int parse_request(struct client_request *request, int clientfd);
void parse_request_h(struct client_request *request, int clientfd);
struct cache_cell *allocate_cell();
void set_cell(struct cache_cell *cell, char *request_line, 
			  char *content, int size);
void free_cell(struct cache_cell *cell);
int open_serverfd_h(struct client_request *request, int clientfd);
void clienterror(int fd, char *cause, char *errnum,
                                 char *shortmsg, char *longmsg);
handler_t *Signal(int signum, handler_t *handler); 
void *thread(void *vargp);
void hit_handler(int clientfd, struct cache_cell *cell, 
				 struct client_request *request);
void miss_handler(int clientfd, struct client_request *request);
struct cache_cell *search_cell(char *request_line);
void remove_from_list(struct cache_cell *cell);
void add_to_list(struct cache_cell *cell);
void close_connection(struct client_request *request, int clientfd, int serverfd);

void Pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t *attr);
void Pthread_mutex_lock(pthread_mutex_t *mutex);
void Pthread_mutex_unlock(pthread_mutex_t *mutex);
void Pthread_rwlock_init(pthread_rwlock_t *lock, const pthread_rwlockattr_t *attr);
void Pthread_rwlock_rdlock(pthread_rwlock_t *lock);
void Pthread_rwlock_wrlock(pthread_rwlock_t *lock);
void Pthread_rwlock_unlock(pthread_rwlock_t *lock);
void Pthread_rwlock_destroy(pthread_rwlock_t *lock);


/* global variables */
struct cache_cell *head;    /* points to the first element of cache */
unsigned long cache_time;   /* current relative time */
int cache_size;             /* total size of elements in cache */
pthread_mutex_t malloc_mutex;  /* mutex that locks malloc */
pthread_mutex_t free_mutex;    /* mutex that locks free */
pthread_mutex_t open_mutex;    /* mutex that locks open_clientfd */
pthread_mutex_t time_mutex;    /* mutex that locks cache_time */
pthread_mutex_t dup_mutex;     /* mutex that prevents duplicates in cache */
pthread_rwlock_t cache_lock;   /* rwlock that locks the whole cache */


                         
int main (int argc, char *argv [])
{
	int proxy_port, listenfd, clientlen, clientfd;
	struct sockaddr_in clientaddr;
	pthread_t tid;
	
	/* parse command line arguments */
	if (argc != 2)
	{
		fprintf(stderr, "This is a web proxy.\nUsage:\n$./proxy port\n");
		exit(1);
	}
	proxy_port = atoi(argv[1]);
	
	/* ignore SIGPIPE signal */
	Signal(SIGPIPE, SIG_IGN);
	
	/* initialize global variables */
	head = NULL;
	cache_time = 0;
	cache_size = 0;
	Pthread_mutex_init(&malloc_mutex, NULL);
	Pthread_mutex_init(&free_mutex, NULL);
	Pthread_mutex_init(&open_mutex, NULL);
	Pthread_mutex_init(&time_mutex, NULL);
	Pthread_mutex_init(&dup_mutex, NULL);
	Pthread_rwlock_init(&cache_lock, NULL);
	
	/* open listen file discriptor and listen for connection requests */
	listenfd = Open_listenfd(proxy_port);
	while (1)
	{
		clientlen = sizeof(clientaddr);
		if ((clientfd = accept(listenfd, (SA *)&clientaddr, 
				(socklen_t *)&clientlen)) < 0)
			continue;
		Pthread_create(&tid, NULL, thread, (void *)(long)clientfd);
	}
    return 0;
}


/* thread routine of each connection. thread reads request 
 * from client and if the request is cached, write the cached 
 * content to client; if the request is not cached, it passes
 * the request to server, reads response from server and passes
 * response to client. */
void *thread(void *vargp)
{
	int clientfd;
	struct client_request *request;
	struct cache_cell *cell;

	Pthread_detach(pthread_self());
	clientfd = (int)(long)vargp;
	
	request = allocate_request();
	parse_request_h(request, clientfd);
	
	/* handle hit case */
	if ((cell = search_cell(request->request_line)) != NULL)
		hit_handler(clientfd, cell, request);
	/* handle miss case */
	else
		miss_handler(clientfd, request);
	return NULL;
}


/* allocate_request allocates a client_request structure 
 * and initialize it. */
struct client_request *allocate_request()
{
	struct client_request *request;
	
	Pthread_mutex_lock(&malloc_mutex);
	request = Malloc(sizeof(struct client_request));
	Pthread_mutex_unlock(&malloc_mutex);
	
	(request->request_line)[0] = 0;
	(request->request)[0] = 0;
	(request->host)[0] = 0;
	request->server_port = 0;
	request->request_length = 0;
	return request;
}


/* parse_request_h is a error handling version of parse_request */
void parse_request_h(struct client_request *request, int clientfd)
{
	int ret;
	
	ret = parse_request(request, clientfd);
	if (ret == 0)
		return;
	if (ret == -1) 
		clienterror(clientfd, "URI", "400", "Bad request",
					"Request could not be understood by proxy");
	if (ret == -2) 
		clienterror(clientfd, "method", "501", "Not Implemented",
					"Proxy does not implement this method");
	close_connection(request, clientfd, -1);
}


/* parse_request parses request from client and stores the 
 * information in a client_request structure */
int parse_request(struct client_request *request, int clientfd)
{
	char buf[MAXBUF], method[MAXLINE], uri[MAXLINE], port[MAXLINE], *ptr;
	rio_t rio;
	
	port[0] = 0;
	
	rio_readinitb(&rio, clientfd);
	rio_readlineb(&rio, buf, MAXLINE - 1);
	if (sscanf(buf, "%s %s %*s", method, uri) < 2)
	{
		printf("parsing error %s\n", buf);
		return -1;
	}
	strcpy(request->request_line, buf);
	if (sscanf(uri, "http://%[^:/]:%[^/]/%*s", request->host, port) < 1)
		return -1;
	if (*port == 0)
		request->server_port = 80;
	else
		request->server_port = atoi(port);
	
	if (strcmp(method, "GET"))
		return -2;
	
	sprintf(request->request, "GET %s HTTP/1.0\r\nHost: %s\r\n", uri, request->host);
	
	/* reads all headers */
	while (1)
	{
		rio_readlineb(&rio, buf, MAXLINE - 1);
		
		/* need to change connection header to close */
		if ((ptr = strstr(buf, "Connection:")) != NULL)
		{
			strcat(request->request, "Connection: close\n");
			continue;
		}
		
		/* need to delete keep-alive header */
		if ((ptr = strcasestr(buf, "keep-alive")) != NULL)
			continue;
			
		/* host is already in the header */
		if ((ptr = strstr(buf, "Host:")) != NULL)
			continue;
		strcat(request->request, buf);
		if (*buf == '\r' && *(buf + 1) == '\n')
			break;
	}
	request->request_length = strlen(request->request);
	return 0;
}


/* allocate_cell allocates a new cache cell and initialize data */
struct cache_cell *allocate_cell()
{
	struct cache_cell *cell;

	Pthread_mutex_lock(&malloc_mutex);
	cell = Malloc(sizeof(struct cache_cell));
	Pthread_mutex_unlock(&malloc_mutex);
	
	cell->content = NULL;
	cell->size = 0;
	cell->last_use = 0;
	cell->previous = NULL;
	cell->next = NULL;
	Pthread_rwlock_init(&(cell->lock), NULL);
	return cell;
}


/* set_cell sets the entries of a cache cell */
void set_cell(struct cache_cell *cell, char *request_line, char *content, int size)
{
	Pthread_mutex_lock(&malloc_mutex);
	cell->content = Malloc((size_t)size);
	Pthread_mutex_unlock(&malloc_mutex);
	memcpy(cell->content, content, size);
	strcpy(cell->request_line, request_line);
	cell->size = size;
	return;
}

/* free_cell frees a cell and destroys the lock in that cell */
void free_cell(struct cache_cell *cell)
{
	if (cell->content != NULL)
	{
		Pthread_mutex_lock(&free_mutex);
		free(cell->content);
		Pthread_mutex_unlock(&free_mutex);
	}
	Pthread_rwlock_destroy(&(cell->lock));
	Pthread_mutex_lock(&free_mutex);
	free(cell);
	Pthread_mutex_unlock(&free_mutex);
	return;
}

/* open_serverfd_h is a wrapper for open_clientfd and handles errors. */
int open_serverfd_h(struct client_request *request, int clientfd)
{
	int serverfd;
	
	serverfd = open_clientfd(request->host, request->server_port);
	if (serverfd >= 0)
		return serverfd;
	if (serverfd == -1)  /* socket_error */
		close_connection(request, clientfd, -1);
	if (serverfd == -2)  /* DNS error */
		clienterror(clientfd, "http address", "404", "Not found",
					"Proxy could not find server");
	close_connection(request, clientfd, -1);
	abort(); /* flow should never reach here */
}


/* search_cell searches the cache and returns the cached
 * cell if found and returns NULL if the request cell is 
 * not cached. request_line is treated as the key. The 
 * strategy is to lock the cache for reading. reading from
 * the cache is permitted but writing to the cache is not. 
 * In the hit case, the cache is not unlocked until data is
 * writen to client. */
struct cache_cell *search_cell(char *request_line)
{
	struct cache_cell *ptr;
	
	/* locks the cache for reading */
	Pthread_rwlock_rdlock(&cache_lock);
	for (ptr = head; ptr != NULL; ptr = ptr->next)
	{
		if (!strcmp(ptr->request_line, request_line))
		{
			/* locks the mutex in the cell so that 
			 * other threads cannot access this cell. */
			Pthread_rwlock_wrlock(&(ptr->lock));
			Pthread_mutex_lock(&time_mutex);
			ptr->last_use = cache_time;
			cache_time++;
			Pthread_mutex_unlock(&time_mutex);
			Pthread_rwlock_unlock(&(ptr->lock));
			return ptr;
		}
	}
	/* unlocks the cache immediately in miss case */
	Pthread_rwlock_unlock(&cache_lock);
	return NULL;
}


/* search_cell_variant searches the cache but does not update*/
int search_cell_variant(char *request_line)
{
	struct cache_cell *ptr;
	
	Pthread_rwlock_rdlock(&cache_lock);
	for (ptr = head; ptr != NULL; ptr = ptr->next)
		if (!strcmp(ptr->request_line, request_line))
		{
			Pthread_rwlock_unlock(&cache_lock);
			return 1;
		}
	Pthread_rwlock_unlock(&cache_lock);
	return 0;
}



/* add_to_list takes a cell and adds that cell the
 * the cache list. It locks the cache for writing so
 * that other threads cannot modify it. */
void add_to_list(struct cache_cell *cell)
{
	/* locks the cache for writing */
	Pthread_rwlock_wrlock(&cache_lock);
	/* if there is enough space in the cache,
	 * no eviction is needed. */
	if (cache_size + cell->size <= MAX_CACHE_SIZE)
	{
		cell->next = head;
		if (head != NULL)
			head->previous = cell;
		head = cell;
		cache_size += cell->size;
		cell->last_use = cache_time;
		
		Pthread_mutex_lock(&time_mutex);
		cache_time++;
		Pthread_mutex_unlock(&time_mutex);
	}
	/* if there is not enough space in the cache,
	 * eviction is needed. */
	else
	{
		struct cache_cell *tmp_cell, *ptr;
		int tmp_last_use;
		
		/* remove elements from cache so that there is enough
		 * space in the cache. */
		while (!(cache_size + cell->size <= MAX_CACHE_SIZE))
		{
			tmp_last_use = cache_time + 1;
			for (ptr = head; ptr != NULL; ptr = ptr->next)
				if (ptr->last_use < tmp_last_use)
				{
					tmp_last_use = ptr->last_use;
					tmp_cell = ptr;
				}
			remove_from_list(tmp_cell);
		}
		
		/* add cell to cache */
		cell->next = head;
		if (head != NULL)
			head->previous = cell;
		head = cell;
		cache_size += cell->size;
		cell->last_use = cache_time;
		
		Pthread_mutex_lock(&time_mutex);
		cache_time++;
		Pthread_mutex_unlock(&time_mutex);
	}
	Pthread_rwlock_unlock(&cache_lock);
	return;
}


/* remove_from_list removes a cell from cache
 * and frees that cell */
void remove_from_list(struct cache_cell *cell)
{
	cache_size -= cell->size;

	if (cell->previous != NULL)
		(cell->previous)->next = cell->next;
	else
		head = cell->next;
		
	if (cell->next != NULL)
		(cell->next)->previous = cell->previous;
	
	free_cell(cell);
	return;
}


/* hit_handler handles hit case. It retrieves data from
 * cache and writes data to client. */
void hit_handler(int clientfd, struct cache_cell *cell,
				 struct client_request *request)
{
	if (rio_writen(clientfd, cell->content, cell->size) < 0)
		close_connection(request, clientfd, -1);
	Pthread_rwlock_unlock(&cache_lock);
	close_connection(request, clientfd, -1);
}


/* miss_handler handles miss case. It passes client's request
 * to server and passes server's response to client. If the 
 * response satisfies size requirements, store the response 
 * into cache. */
void miss_handler(int clientfd, struct client_request *request)
{
	int serverfd, length, response_size;
	char buf[MAXBUF], object_buf[MAX_OBJECT_SIZE];
	struct cache_cell *cell;
	rio_t rio_server;
	
	response_size = length = 0;
	
	/* acts as a client and writes request to server */
	Pthread_mutex_lock(&open_mutex);
	serverfd = open_serverfd_h(request, clientfd);
	Pthread_mutex_unlock(&open_mutex);
	rio_readinitb(&rio_server, serverfd);
	
	if (rio_writen(serverfd, request->request, request->request_length) 
					!= request->request_length)
	{
		write(2, "write error\n", strlen("write error\n"));
		close_connection(request, clientfd, serverfd);
	}
	
	/* passes server's response to client */
	while (1)
	{
		if ((length = rio_readnb(&rio_server, buf, MAXBUF)) < 0)
			close_connection(request, clientfd, serverfd);
		if (response_size + length <= MAX_OBJECT_SIZE)
			memcpy(object_buf + response_size, buf, length);
		response_size += length;
		if (rio_writen(clientfd, buf, length) < length)
			break;
		if (length != MAXBUF)
			break;
	}
	
	/* if response satisfies size requirement, store the response 
	 * into cache */
	if (response_size <= MAX_OBJECT_SIZE)
	{
		/* need a mutex to prevent inserting the same cell twice 
		 * into cache in race condition */
		Pthread_mutex_lock(&dup_mutex);
		if (search_cell_variant(request->request_line) == 0)
		{
			cell = allocate_cell();
			set_cell(cell, request->request_line, object_buf, response_size);
			add_to_list(cell);
		}
		Pthread_mutex_unlock(&dup_mutex);
	}
	close_connection(request, clientfd, serverfd);
}


/* close_connection free allocated memory, close files and 
 * terminates current thread */
void close_connection(struct client_request *request, int clientfd, int serverfd)
{
	Pthread_mutex_lock(&free_mutex);
	free(request);
	Pthread_mutex_unlock(&free_mutex);
	if (clientfd >= 0)
		Close(clientfd);
	if (serverfd >= 0)
		Close(serverfd);
	pthread_exit(NULL);
}


/* clienterror sends error message to client */
void clienterror(int fd, char *cause, char *errnum, 
                                 char *shortmsg, char *longmsg) 
{
    char buf[MAXLINE], body[MAXBUF];

    /* Build the HTTP response body */
    sprintf(body, "<html><title>Proxy Error</title>");
    sprintf(body, "%s<body bgcolor=""ffffff"">\r\n", body);
    sprintf(body, "%s%s: %s\r\n", body, errnum, shortmsg);
    sprintf(body, "%s<p>%s: %s\r\n", body, longmsg, cause);
    sprintf(body, "%s<hr><em>Web Proxy</em>\r\n", body);

    /* Print the HTTP response */
    sprintf(buf, "HTTP/1.0 %s %s\r\n", errnum, shortmsg);
    rio_writen(fd, buf, strlen(buf));
    sprintf(buf, "Content-type: text/html\r\n");
    Rio_writen(fd, buf, strlen(buf));
    sprintf(buf, "Content-length: %d\r\n\r\n", (int)strlen(body));
    Rio_writen(fd, buf, strlen(buf));
    Rio_writen(fd, body, strlen(body));
}




/* error-handling wrapper function for pthread_mutex_lock */
void Pthread_mutex_lock(pthread_mutex_t *mutex)
{
	if (pthread_mutex_lock(mutex) != 0)
		unix_error("pthread_mutex_lock error");
	return;
}

/* error-handling wrapper function for pthread_mutex_unlock */
void Pthread_mutex_unlock(pthread_mutex_t *mutex)
{
	if (pthread_mutex_unlock(mutex) != 0)
		unix_error("pthread_mutex_unlock error");
	return;
}

/* error-handling wrapper function for pthread_mutex_init */
void Pthread_mutex_init(pthread_mutex_t *mutex, const pthread_mutexattr_t *attr)
{
	if (pthread_mutex_init(mutex, attr) != 0)
		unix_error("pthread_mutex_init error");
	return;
}

/* error-handling wrapper function for pthread_rwlock_init */
void Pthread_rwlock_init(pthread_rwlock_t *lock, const pthread_rwlockattr_t *attr)
{
	if (pthread_rwlock_init(lock, attr) != 0)
		unix_error("pthread_rwlock_init error");
	return;
}

/* error-handling wrapper function for pthread_rwlock_rdlock */
void Pthread_rwlock_rdlock(pthread_rwlock_t *lock)
{
	if (pthread_rwlock_rdlock(lock) != 0)
		unix_error("pthread_rwlock_rdlock error");
	return;
}

/* error-handling wrapper function for pthread_rwlock_wrlock */
void Pthread_rwlock_wrlock(pthread_rwlock_t *lock)
{
	if (pthread_rwlock_wrlock(lock) != 0)
		unix_error("pthread_rwlock_wrlock error");
	return;
}

/* error-handling wrapper function for pthread_rwlock_unlock */
void Pthread_rwlock_unlock(pthread_rwlock_t *lock)
{
	if (pthread_rwlock_unlock(lock) != 0)
		unix_error("pthread_rwlock_unlock error");
	return;
}

/* error-handling wrapper function for pthread_rwlock_destroy */
void Pthread_rwlock_destroy(pthread_rwlock_t *lock)
{
	if (pthread_rwlock_destroy(lock) != 0)
		unix_error("pthread_rwlock_destroy error");
	return;
}