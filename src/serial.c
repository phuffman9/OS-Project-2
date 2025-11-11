#include <dirent.h> 
#include <stdio.h> 
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_THREADS 20

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
} thread_arg_t;


void *compress_file_thread(void *arg){
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

	deflateEnd(&strm);
	free(targ->file_path);
	free(targ);
	
	pthread_exit(NULL);
}


int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

int compress_directory(char *directory_name) {
	pthread_t p1, p2;
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;

	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return 0;
	}

	// create sorted list of text files
	while ((dir = readdir(d)) != NULL) {
		files = realloc(files, (nfiles+1)*sizeof(char *));
		assert(files != NULL);

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
	thread_data_t *thread_data = calloc(nfiles, sizeof(thread_data_t));
	assert(thread_data != NULL);
	
	// Allocate input buffers for each file
	for(int i = 0; i < nfiles; i++) {
		thread_data[i].input_buffer = malloc(BUFFER_SIZE);
		assert(thread_data[i].input_buffer != NULL);
	}

	// Process files in batches of MAX_THREADS
	pthread_t threads[MAX_THREADS];
	int file_idx = 0;
	
	while(file_idx < nfiles) {
		// Determine how many threads to launch in this batch
		int batch_size = (nfiles - file_idx < MAX_THREADS) ? (nfiles - file_idx) : MAX_THREADS;
		
		// Launch threads for this batch
		for(int i = 0; i < batch_size; i++) {
			int current_file = file_idx + i;
			
			// Build full path
			int len = strlen(directory_name) + strlen(files[current_file]) + 2;
			char *full_path = malloc(len * sizeof(char));
			assert(full_path != NULL);
			strcpy(full_path, directory_name);
			strcat(full_path, "/");
			strcat(full_path, files[current_file]);
			
			// Create thread argument
			thread_arg_t *arg = malloc(sizeof(thread_arg_t));
			assert(arg != NULL);
			arg->file_path = full_path;
			arg->index = current_file;
			arg->thread_data = &thread_data[current_file];
			
			// Launch thread
			pthread_create(&threads[i], NULL, compress_file_thread, arg);
		}
		
		// Wait for all threads in this batch to complete
		for(int i = 0; i < batch_size; i++) {
			pthread_join(threads[i], NULL);
		}
		
		file_idx += batch_size;
	}

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
