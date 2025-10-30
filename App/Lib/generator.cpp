/* @version $Id: generator.c 3031 2012-12-07 14:37:54Z bcagri $ */

#include <stdio.h>              /* perror */
#include <stdlib.h>             /* posix_memalign */
#include <time.h>               /* time() */
#include <string.h>             /* memcpy() */
#include <stdint.h>


#include "genzipf.h"            /* gen_zipf() */
#include "generator.h"          /* create_relation_*() */
#include "data-types.h"
#include "Logger.h"
#include "utility.h"
#include "parallel_sort.h"

/* return a random number in range [0,N] */
#define RAND_RANGE(N) ((double)rand() / ((double)RAND_MAX + 1) * (N))
#define RAND_RANGE48(N,STATE) ((double)nrand48(STATE)/((double)RAND_MAX+1)*(N))

/* Uncomment the following to persist input relations to disk. */
/* #define PERSIST_RELATIONS 1 */

/** An experimental feature to allocate input relations numa-local */
int numalocalize;
int nthreads;

static int seeded = 0;
static unsigned int seedValue;

static void sort_relation(relation_t *rel)
{
    std::size_t position;
    if (misc::sorted(rel, position))
    {
        return;
    }
    internal::psort(rel, 4);
    if (!misc::sorted(rel, position))
    {
        logger(ERROR, "Failed sorting relation at position %lu", position);
        exit(EXIT_FAILURE);
    }
    logger(DBG, "Relation sorted");
}

void *
alloc_aligned(size_t size)
{
    void * ret;
    int rv;
    size_t CACHE_LINE_SIZE = 64;
    rv = posix_memalign((void**)&ret, CACHE_LINE_SIZE, size);

    if (rv) {
        perror("generator: alloc_aligned() failed: out of memory");
        return 0;
    }

//    /** Not an elegant way of passing whether we will numa-localize, but this
//        feature is experimental anyway. */
//    if(numalocalize) {
//        struct row_t * mem = (struct row_t *) ret;
//        uint32_t ntuples = size / sizeof(struct row_t*);
//        numa_localize(mem, ntuples, nthreads);
//    }

    return ret;
}

void 
seed_generator(unsigned int seed) 
{
    srand(seed);
    seedValue = seed;
    seeded = 1;
}

/** Check wheter seeded, if not seed the generator with current time */
static void
check_seed()
{
    if(!seeded) {
        seedValue = (unsigned int) time(NULL);
        srand(seedValue);
        seeded = 1;
    }
}


/** 
 * Shuffle tuples of the relation using Knuth shuffle.
 * 
 * @param relation 
 */
void 
knuth_shuffle(struct table_t * relation)
{
    uint64_t i;
    for (i = relation->num_tuples - 1; i > 0; i--) {
        int64_t  j                  = (int64_t) RAND_RANGE((double)i);
        type_key tmp                = relation->tuples[i].key;
        relation->tuples[i].key     = relation->tuples[j].key;
        relation->tuples[j].key     = tmp;
        type_value tmpv             = relation->tuples[i].payload;
        relation->tuples[i].payload = relation->tuples[j].payload;
        relation->tuples[j].payload = tmpv;
    }
}

void
knuth_shuffle_keys(int32_t *array, int32_t array_size, int32_t shuff_size)
{
    if (array_size > 1) {
        int32_t i;
        for (i = 0; i < shuff_size; i++)
        {
            int32_t j = (int32_t) RAND_RANGE(array_size);
            int32_t t = array[j];
            array[j] = array[i];
            array[i] = t;
        }
    }
}

void 
knuth_shuffle48(struct table_t * relation, unsigned short * state)
{
    uint64_t i;
    for (i = relation->num_tuples - 1; i > 0; i--) {
        int32_t  j              = (int32_t) RAND_RANGE48((double)i, state);
        type_key tmp            = relation->tuples[i].key;
        relation->tuples[i].key = relation->tuples[j].key;
        relation->tuples[j].key = tmp;
    }
}

/**
 * Generate unique tuple IDs with Knuth shuffling
 * relation must have been allocated
 */
void
random_unique_gen(struct table_t *rel, unsigned int rate)
{
    uint64_t i;
    unsigned long t = 0;
    unsigned long range = (unsigned long) (2.e9 / rate);

    for (i = 0; i < rel->num_tuples; i++) {
        rel->tuples[i].key = (type_key)(i+1);
        rel->tuples[i].payload = (type_value)(i+1);
    }

    /* randomly shuffle elements */
    knuth_shuffle(rel);

    /* assign timestamps to tuples */
    for (i = 0; i < rel->num_tuples; i++) {
        t = t + (random() % range);
        rel->tuples[i].ts = (struct timespec_t) { .tv_sec = t / 1000000000L,
                .tv_nsec = t % 1000000000L};
    }

    /* add one dummy tuple with an "infinity" timestamp */
    rel->tuples[rel->num_tuples].ts = (struct timespec_t) { .tv_sec  = UINT64_MAX,
            .tv_nsec = UINT64_MAX };
}

/**
 * Generate unique tuple IDs with Knuth shuffling
 * relation must have been allocated
 */
void
random_unique_gen(struct table_t *rel)
{
    uint64_t i;

    for (i = 0; i < rel->num_tuples; i++) {
        rel->tuples[i].key = (type_key)(i+1);
        rel->tuples[i].payload = (type_value)(i+1);
    }

    /* randomly shuffle elements */
    knuth_shuffle(rel);
}

void
random_unique_gen_maxid(struct table_t *rel, uint32_t maxid)
{
    uint32_t i;
    double jump = (double)maxid / (double)rel->num_tuples;

    double id = maxid == 0 ? 0 : 1;
    for (i = 0; i < rel->num_tuples; i++) {
        rel->tuples[i].key = (uint32_t) id;
        id += jump;
    }

    /* randomly shuffle elements */
    knuth_shuffle(rel);
}

/**
 * Generate unique tuple IDs with Knuth shuffling
 * relation must have been allocated
 */
void
random_unique_gen_with_keys(struct table_t *rel, int32_t *keys, int64_t keys_size)
{
    uint64_t i;

    for (i = 0; i < rel->num_tuples; i++) {
//        rel->tuples[i].key = (i+1);
        uint32_t key = (keys_size > 0) ? (uint32_t) keys[i % (keys_size-1)] : INT32_MAX;
        rel->tuples[i].key = key;
    }

    /* randomly shuffle elements */
    knuth_shuffle(rel);
}

struct create_arg_t {
    struct table_t rel;
    uint32_t firstkey;
};

typedef struct create_arg_t create_arg_t;

/**
 * Write relation to a file.
 */
void
write_relation(struct table_t * rel, const char * filename)
{
    FILE * fp = fopen(filename, "wN");
    uint32_t i;

    fprintf(fp, "#TS, KEY, VAL\n");

    for (i = 0; i < rel->num_tuples; i++) {
        fprintf(fp, "%ld.%.9ld %d %d\n",
                rel->tuples[i].ts.tv_sec, rel->tuples[i].ts.tv_nsec, rel->tuples[i].key, rel->tuples[i].payload);
    }

    fclose(fp);
}

/**
 * Generate tuple IDs -> random distribution
 * relation must have been allocated
 */
void 
random_gen(struct table_t *rel, const uint32_t maxid, unsigned int rate)
{
    uint32_t i;
    unsigned long t = 0;
    /* interval between two tuples is 0..range nsec */
    unsigned long range = (unsigned long) (2.e9 / rate);
    for (i = 0; i < rel->num_tuples; i++) {
        t = t + (random() % range);
        rel->tuples[i].ts = (struct timespec_t) { .tv_sec = t / 1000000000L,
                .tv_nsec = t % 1000000000L};
        long int r = random();
        type_key k = (type_key) (r%maxid) + 1;
        rel->tuples[i].key = k;
        rel->tuples[i].payload = rel->tuples[i].key;
    }

    /* add one dummy tuple with an "infinity" timestamp */
    rel->tuples[rel->num_tuples].ts = (struct timespec_t) { .tv_sec  = UINT64_MAX,
            .tv_nsec = UINT64_MAX };
}

int
create_relation_from_file(relation_t *relation, char* filename, unsigned int rate)
{
    // find out the number of lines (tuples) in the file
    FILE *fp;
    uint64_t lines = 0;
    char c;
    fp = fopen(filename, "r");
    ssize_t line_size;
    char *line_buf;
    size_t line_buf_size = 0;
    unsigned long t = 0;
    unsigned long range = (unsigned long) (2.e9 / rate);

    if (fp == nullptr)
    {
        logger(ERROR, "Could not open file %s", filename);
        return -1;
    }

    for (c = (char)getc(fp); c != EOF; c = (char)getc(fp))
    {
        if (c == '\n')
        {
            lines++;
        }
    }
    fclose(fp);
    logger(DBG, "File %s has %lu lines", filename, lines);

    relation->num_tuples = (uint32_t) lines;
    relation->tuples = (struct row_t*) alloc_aligned((relation->num_tuples + 1) * sizeof(struct row_t));

    if (!relation->tuples)
    {
        logger(ERROR, "Out of memory");
        return -1;
    }

    // read content of file and set keys in the relation
    fp = fopen(filename, "r");
    line_size = getline(&line_buf, &line_buf_size, fp);

    uint32_t i = 0;
    while (line_size >= 0)
    {
        uint32_t tmp = atoi(line_buf);
        if (tmp == 0)
        {
            logger(ERROR, "Line can not be parsed to int: %s", line_buf);
        }

        t = t + (random() % range);
        relation->tuples[i].ts = (struct timespec_t) { .tv_sec = t / 1000000000L,
                .tv_nsec = t % 1000000000L};
        relation->tuples[i].key = tmp;
        relation->tuples[i].payload = tmp;
        i++;
        line_size = getline(&line_buf, &line_buf_size, fp);
    }
    /* add one dummy tuple with an "infinity" timestamp */
    relation->tuples[i].ts = (struct timespec_t) { .tv_sec  = UINT64_MAX,
            .tv_nsec = UINT64_MAX };

    free(line_buf);
    line_buf = nullptr;
    fclose(fp);
    knuth_shuffle(relation);
    return 0;
}

int 
create_relation_pk(struct table_t *relation, uint32_t num_tuples, unsigned int rate)
{
    check_seed();

    relation->num_tuples = num_tuples;
    relation->tuples = (struct row_t*) alloc_aligned((relation->num_tuples + 1) * sizeof(struct row_t));
    if (!relation->tuples) { 
        perror("out of memory");
        return -1; 
    }
  
    random_unique_gen(relation, rate);

#ifdef PERSIST_RELATIONS
    write_relation(relation, "R.tbl");
#endif

    return 0;
}


int 
create_relation_fk(struct table_t *relation, uint32_t num_tuples, const uint32_t maxid, unsigned int rate)
{
    uint32_t i, iters;
    uint32_t remainder;
    struct table_t tmp;

    check_seed();

    relation->num_tuples = num_tuples;
    relation->tuples = (struct row_t*) alloc_aligned((relation->num_tuples + 1) * sizeof(struct row_t));

    if (!relation->tuples) { 
        perror("out of memory");
        return -1; 
    }
  
    /* alternative generation method */
    iters = num_tuples / maxid;
    for(i = 0; i < iters; i++){
        tmp.num_tuples = maxid;
        tmp.tuples = relation->tuples + maxid * i;
        random_unique_gen(&tmp);
    }

    /* if num_tuples is not an exact multiple of maxid */
    remainder = num_tuples % maxid;
    if(remainder > 0) {
        tmp.num_tuples = remainder;
        tmp.tuples = relation->tuples + maxid * iters;
        random_unique_gen(&tmp);
    }

    unsigned long range = (unsigned long) (2.e9 / rate);
    unsigned long t = 0;

    /* assign timestamps to tuples */
    for (i = 0; i < relation->num_tuples; i++) {
        t = t + (random() % range);
        relation->tuples[i].ts = (struct timespec_t) { .tv_sec = t / 1000000000L,
                .tv_nsec = t % 1000000000L};
    }

    /* add one dummy tuple with an "infinity" timestamp */
    relation->tuples[relation->num_tuples].ts = (struct timespec_t) { .tv_sec  = UINT64_MAX,
            .tv_nsec = UINT64_MAX };

    return 0;
}

/** 
 * Create a foreign-key relation using the given primary-key relation and
 * foreign-key relation size. Keys in pkrel is randomly distributed in the full
 * integer range.
 * 
 * @param fkrel [output] foreign-key relation
 * @param pkrel [input] primary-key relation
 * @param num_tuples 
 * 
 * @return 
 */
int 
create_relation_fk_from_pk(struct table_t *fkrel, struct table_t *pkrel,
                           int64_t num_tuples, int sorted)
{
    int rv, i, iters, remainder;

    size_t CACHE_LINE_SIZE = 64;
    rv = posix_memalign((void**)&fkrel->tuples, CACHE_LINE_SIZE,
                        num_tuples * sizeof(struct row_t));

    if (rv) { 
        perror("aligned alloc failed: out of memory");
        return 0; 
    }

    fkrel->num_tuples = (uint32_t) num_tuples;

    /* alternative generation method */
    iters = (int)(num_tuples / pkrel->num_tuples);
    for(i = 0; i < iters; i++){
        memcpy(fkrel->tuples + i * pkrel->num_tuples, pkrel->tuples,
               pkrel->num_tuples * sizeof(struct row_t));
    }

    /* if num_tuples is not an exact multiple of pkrel->size */
    remainder = (int)(num_tuples % pkrel->num_tuples);
    if(remainder > 0) {
        memcpy(fkrel->tuples + i * pkrel->num_tuples, pkrel->tuples,
               remainder * sizeof(struct row_t));
    }
    if (sorted)
    {
        sort_relation(fkrel);
    } else {
        knuth_shuffle(fkrel);
    }



    return 0;
}

int create_relation_nonunique(struct table_t *relation, uint32_t num_tuples,
                              const uint32_t maxid, unsigned int rate)
{
    check_seed();

    relation->num_tuples = num_tuples;
    relation->tuples = (struct row_t*) alloc_aligned((relation->num_tuples + 1) * sizeof(struct row_t));

    if (!relation->tuples) {
        perror("out of memory");
        return -1;
    }

    random_gen(relation, maxid, rate);

    return 0;
}

int
create_relation_zipf(struct table_t * relation, uint32_t num_tuples,
                     const uint32_t maxid, const double zipf_param, unsigned int rate)
{
    check_seed();

    relation->num_tuples = num_tuples;
    relation->tuples = (struct row_t*) alloc_aligned((relation->num_tuples + 1) * sizeof(struct row_t));

    if (!relation->tuples) {
        perror("out of memory");
        return -1;
    }

    gen_zipf(num_tuples, maxid, zipf_param, &relation->tuples);

    unsigned long t = 0;
    /* interval between two tuples is 0..range nsec */
    unsigned long range = (unsigned long) (2.e9 / rate);
    for (uint32_t i = 0; i < relation->num_tuples; i++) {
        t = t + (random() % range);
        relation->tuples[i].ts = (struct timespec_t) { .tv_sec = t / 1000000000L,
                .tv_nsec = t % 1000000000L};
//        long int r = random();
//        type_key k = (type_key) (r%maxid) + 1;
//        relation->tuples[i].key = k;
//        relation->tuples[i].payload = relation->tuples[i].key;
    }

    /* add one dummy tuple with an "infinity" timestamp */
    relation->tuples[relation->num_tuples].ts = (struct timespec_t) { .tv_sec  = UINT64_MAX,
            .tv_nsec = UINT64_MAX };

    return 0;
}

void 
delete_relation(struct table_t * rel)
{
    /* clean up */
    free(rel->tuples);
}
