// group 
// Peyton Huffman, Tiago Silvestre
// A multi-threaded implemetation of the compression tool using pthreads library
#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_THREADS 20




typedef struct{
	void (*function)(void *);
	void *arg;
} thread_task_t;

typedef struct{
	pthread_t *threads;
	int max_threads;
	int num_threads;
	thread_task_t *task_queue;
	int queue_size;
	int queue_capacity;
	pthread_mutex_t queue_mutex;
	pthread_cond_t queue_cond;
	int shutdown;
} thread_pool_t;

void *thread_worker(void *arg);
void thread_pool_add_task(thread_pool_t *pool, void (*function)(void *), void *arg);

//initialize thread pool by creating 20 threads calling the thread worker function
void thread_pool_init(thread_pool_t *pool, int max_threads){
	pool->max_threads = max_threads;
	pool->num_threads = 0;
	pool->task_queue = NULL;
	pool->queue_size = 0;
	pool->queue_capacity = 0;
	pool->shutdown = 0;
	pthread_mutex_init(&pool->queue_mutex, NULL);
	pthread_cond_init(&pool->queue_cond, NULL);
	pool->threads = malloc(max_threads * sizeof(pthread_t));
	for(int i = 0; i < max_threads; i++){
		pthread_create(&pool->threads[i], NULL, thread_worker, (void *) pool);
	}
}
//destroy thread pool and free memory
void thread_pool_destroy(thread_pool_t *pool){
	pthread_mutex_lock(&pool->queue_mutex);
    pool->shutdown = 1;
    pthread_cond_broadcast(&pool->queue_cond);
    pthread_mutex_unlock(&pool->queue_mutex);

	
	for(int i = 0; i < pool->max_threads; i++){
		pthread_join(pool->threads[i], NULL);
	}
	pthread_mutex_destroy(&pool->queue_mutex);
	pthread_cond_destroy(&pool->queue_cond);
	free(pool->task_queue);
	free(pool->threads);
}
//add task to thread pool
void thread_pool_add_task(thread_pool_t *pool, void (*function)(void *), void *arg){
	pthread_mutex_lock(&pool->queue_mutex);
	if(pool->queue_size >= pool->queue_capacity){
		pool->queue_capacity = (pool->queue_capacity == 0) ? 1 : pool->queue_capacity * 2;
		pool->task_queue = realloc(pool->task_queue, pool->queue_capacity * sizeof(thread_task_t));
	}
	//add task to queue
	pool->task_queue[pool->queue_size].function = function;
	pool->task_queue[pool->queue_size].arg = arg;
	pool->queue_size++;

	// wake up any sleeping threads waiting for a task to be added
	pthread_cond_signal(&pool->queue_cond);
	pthread_mutex_unlock(&pool->queue_mutex);
}

//thread worker called by each thread
//waits for tasks to be added
void *thread_worker(void *arg){
	thread_pool_t *pool = (thread_pool_t *) arg;
	while(1){
		pthread_mutex_lock(&pool->queue_mutex);
		// wait for task
		while(pool->queue_size == 0 && !pool->shutdown){
			pthread_cond_wait(&pool->queue_cond, &pool->queue_mutex);
		}
		if(pool->shutdown && pool->queue_size == 0){
			pthread_mutex_unlock(&pool->queue_mutex);
			pthread_exit(NULL);
		}
		thread_task_t task = pool->task_queue[0];
		//shift task queue
		for(int i = 1; i < pool->queue_size; i++){
			pool->task_queue[i-1] = pool->task_queue[i];
		}
		pool->queue_size--;
		pthread_mutex_unlock(&pool->queue_mutex);
		task.function(task.arg);
	}
	return NULL;
}
//create results struct that will store output for each thread
//will have one for each file for writing results to zipped file
typedef struct{
	unsigned char *output_buffer;
	unsigned char *input_buffer;
	int nbytes;
	int nbytes_zipped;
} thread_data_t;

//create argument struct
typedef struct{
	char *file_path;
	int index;
	thread_data_t *thread_data;
	int *completed_threads;
	pthread_mutex_t *completed_mutex;
	pthread_cond_t *completed_cond;
} thread_arg_t;


void compress_file_thread(void *arg){
	thread_arg_t *targ = (thread_arg_t *) arg;
	

	// allocate output buffer
	targ->thread_data->output_buffer = malloc(BUFFER_SIZE);
	assert(targ->thread_data->output_buffer != NULL);

	// open input file
	FILE *f_in = fopen(targ->file_path, "r");
	assert(f_in != NULL);

	// read input file
	targ->thread_data->nbytes = fread(targ->thread_data->input_buffer, sizeof(unsigned char), BUFFER_SIZE, f_in);
	fclose(f_in);

	// compress data as in original func
	z_stream strm;
	int ret = deflateInit(&strm, 9);
	assert(ret == Z_OK);
	strm.avail_in = targ->thread_data->nbytes;
	strm.next_in = targ->thread_data->input_buffer;
	strm.avail_out = BUFFER_SIZE;
	strm.next_out = targ->thread_data->output_buffer;

	ret = deflate(&strm, Z_FINISH);
	assert(ret == Z_STREAM_END);

	// store compressed size
	targ->thread_data->nbytes_zipped = BUFFER_SIZE - strm.avail_out;
	pthread_mutex_lock(targ->completed_mutex);
	(*targ->completed_threads)++;
	pthread_cond_signal(targ->completed_cond);
	pthread_mutex_unlock(targ->completed_mutex);

	deflateEnd(&strm);
	free(targ->file_path);
	free(targ);
	
}


int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

int compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;
	
	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return 0;
	}
	int capacity = 200;
	files = malloc(capacity * sizeof(char *));
	assert(files != NULL);
	// create sorted list of text files
	while ((dir = readdir(d)) != NULL) {
		//check if there is more files than capacity -> add more space
		if(nfiles >= capacity) {
			capacity *= 2;
			files = realloc(files, capacity * sizeof(char *));
			assert(files != NULL);
		}

		int len = strlen(dir->d_name);
		if(dir->d_name[len-4] == '.' && dir->d_name[len-3] == 't' && dir->d_name[len-2] == 'x' && dir->d_name[len-1] == 't') {
			files[nfiles] = strdup(dir->d_name);
			assert(files[nfiles] != NULL);

			nfiles++;
		}
	}
	closedir(d);
	qsort(files, nfiles, sizeof(char *), cmp);

	// Allocate thread data and results arrays
	if(nfiles == 0){
		return 0;
	}
	thread_data_t *thread_data = calloc(nfiles, sizeof(thread_data_t));
	thread_pool_t *pool = malloc(sizeof(thread_pool_t));
	thread_pool_init(pool, MAX_THREADS);
	assert(thread_data != NULL);
	assert(pool != NULL);
	
	// Allocate input buffers for each file
	for(int i = 0; i < nfiles; i++) {
		thread_data[i].input_buffer = malloc(BUFFER_SIZE);
		assert(thread_data[i].input_buffer != NULL);
	}
	int completed_threads = 0;
	pthread_mutex_t completed_mutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_cond_t completed_cond = PTHREAD_COND_INITIALIZER;
	// create threads for thread pool
	for(int i =0; i < nfiles; i++){
		// Build full path
		int len = strlen(directory_name) + strlen(files[i]) + 2;
		char *full_path = malloc(len * sizeof(char));
		assert(full_path != NULL);
		strcpy(full_path, directory_name);
		strcat(full_path, "/");
		strcat(full_path, files[i]);

		// Create thread argument
		thread_arg_t *arg = malloc(sizeof(thread_arg_t));
		assert(arg != NULL);
		arg->file_path = full_path;
		arg->index = i;
		arg->thread_data = &thread_data[i];
		arg->completed_mutex = &completed_mutex;
		arg->completed_threads = &completed_threads;
		arg->completed_cond = &completed_cond;

		//add task to thread pool
		thread_pool_add_task(pool, compress_file_thread, arg);
	}
	
	//while(file_idx < nfiles) {
	//	// Determine how many threads to launch in this batch
	//	int batch_size = (nfiles - file_idx < MAX_THREADS) ? (nfiles - file_idx) : MAX_THREADS;
	//	
	//	// Launch threads for this batch
	//	for(int i = 0; i < batch_size; i++) {
	//		int current_file = file_idx + i;
	//		
	//		// Build full path
	//		int len = strlen(directory_name) + strlen(files[current_file]) + 2;
	//		char *full_path = malloc(len * sizeof(char));
	//		assert(full_path != NULL);
	//		strcpy(full_path, directory_name);
	//		strcat(full_path, "/");
	//		strcat(full_path, files[current_file]);
	//		
	//		// Create thread argument
	//		thread_arg_t *arg = malloc(sizeof(thread_arg_t));
	//		assert(arg != NULL);
	//		arg->file_path = full_path;
	//		arg->index = current_file;
	//		arg->thread_data = &thread_data[current_file];
	//		
	//		// Launch thread
	//		pthread_create(&threads[i], NULL, compress_file_thread, arg);
	//	}
	//	
	//	// Wait for all threads in this batch to complete
	//	for(int i = 0; i < batch_size; i++) {
	//		pthread_join(threads[i], NULL);
	//	}
	//	
	//	file_idx += batch_size;
	//}

	// Wait for all threads to complete
	pthread_mutex_lock(&completed_mutex);
	while(completed_threads < nfiles){
		//busy wait
		pthread_cond_wait(&completed_cond, &completed_mutex);
	}
	pthread_mutex_unlock(&completed_mutex);

	thread_pool_destroy(pool);
	free(pool);
	pthread_mutex_destroy(&completed_mutex);
	pthread_cond_destroy(&completed_cond);

	// Write all results to output file in lexicographical order
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("text.tzip", "w");
	assert(f_out != NULL);
	for(int i = 0; i < nfiles; i++) {
		// Write compressed data
		fwrite(&thread_data[i].nbytes_zipped, sizeof(int), 1, f_out);
		fwrite(thread_data[i].output_buffer, sizeof(unsigned char), thread_data[i].nbytes_zipped, f_out);
		
		// Update totals
		total_in += thread_data[i].nbytes;
		total_out += thread_data[i].nbytes_zipped;
		
		// Free buffers
		free(thread_data[i].input_buffer);
		free(thread_data[i].output_buffer);
	}
	fclose(f_out);
	
	//free thread data array
	free(thread_data);

	printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

	// release list of files
	for(int i = 0; i < nfiles; i++)
		free(files[i]);
	free(files);

	// do not modify the main function after this point!
	return 0;
}
