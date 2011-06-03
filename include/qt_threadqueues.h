#ifndef QT_THREADQUEUES_H
#define QT_THREADQUEUES_H

#include "qt_atomics.h"

typedef struct _qt_threadqueue_node qt_threadqueue_node_t;

struct qt_threadqueue_s {
#ifdef QTHREAD_MUTEX_INCREMENT
    qt_threadqueue_node_t *head;
    qt_threadqueue_node_t *tail;
    QTHREAD_FASTLOCK_TYPE head_lock;
    QTHREAD_FASTLOCK_TYPE tail_lock;
    QTHREAD_FASTLOCK_TYPE advisory_queuelen_m;
#else
    volatile qt_threadqueue_node_t *volatile head;
    volatile qt_threadqueue_node_t *volatile tail;
#ifdef QTHREAD_CONDWAIT_BLOCKING_QUEUE
    volatile aligned_t fruitless;
    pthread_mutex_t lock;
    pthread_cond_t notempty;
#endif				/* CONDWAIT */
#endif				/* MUTEX_INCREMENT */
    /* the following is for estimating a queue's "busy" level, and is not
     * guaranteed accurate (that would be a race condition) */
    volatile saligned_t advisory_queuelen;
    qthread_shepherd_t *creator_ptr;
#ifdef QTHREAD_MULTITHREADED_SHEPHERDS
    /* used for the work stealing queue implementation */
    QTHREAD_FASTLOCK_TYPE qlock;
    long qlength;
    long qlength_stealable; /* number of stealable tasks on queue - stop steal attempts
			     * that will fail because tasks cannot be moved - 4/1/11 AKP
			     */ 
#endif
};

#ifdef QTHREAD_MULTITHREADED_SHEPHERDS
# define QMS_ARG(x) , x
#else
# define QMS_ARG(x)
#endif

void qt_threadqueue_init_pool(qt_mpool *pool);
void qt_threadqueue_destroy_pool(qt_mpool *pool);

qt_threadqueue_t *qt_threadqueue_new(qthread_shepherd_t * shepherd);
void qt_threadqueue_free(qt_threadqueue_t * q);
void qt_threadqueue_enqueue(qt_threadqueue_t * q, qthread_t * t,
			    qthread_shepherd_t * shep);
qthread_t *qt_threadqueue_dequeue(qt_threadqueue_t * q);
qthread_t *qt_threadqueue_dequeue_blocking(qt_threadqueue_t *
					   q QMS_ARG(size_t active));
#ifdef QTHREAD_MULTITHREADED_SHEPHERDS
/* Functions for work stealing functionality */
void qt_threadqueue_enqueue_yielded(qt_threadqueue_t * q, qthread_t * t,
				    qthread_shepherd_t * shep);
void qt_threadqueue_enqueue_multiple(qt_threadqueue_t * q,
				     qt_threadqueue_node_t * first,
				     qthread_shepherd_t * shep);
qt_threadqueue_node_t *qt_threadqueue_dequeue_steal(qt_threadqueue_t * q);
qt_threadqueue_node_t *qt_threadqueue_dequeue_specific(qt_threadqueue_t * q,
						       void *value);
void qthread_steal_stat(void);
#else
# define qt_threadqueue_enqueue_yielded(q,t,s) qt_threadqueue_enqueue((q),(t),(s))
#endif

#endif
