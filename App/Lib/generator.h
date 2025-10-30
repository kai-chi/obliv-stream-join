/**
 * @file    generator.h
 * @author  Cagri Balkesen <cagri.balkesen@inf.ethz.ch>
 * @date    Fri May 18 14:05:07 2012
 * @version $Id: generator.h 3017 2012-12-07 10:56:20Z bcagri $
 * 
 * @brief  Provides methods to generate data sets of various types
 * 
 */

#ifndef GENERATOR_H
#define GENERATOR_H

#include "data-types.h"

/** 
 * @defgroup DataGeneration Data Set Generation
 * @{
 */

/**
 * Seed the random number generator before calling create_relation_xx. If not
 * called, then generator will be initialized with the time of the call which
 * produces different random numbers from run to run.
 */
void 
seed_generator(unsigned int seed);

/**
 * Create relation with non-unique keys uniformly distributed between [0, maxid]
 */
int
create_relation_nonunique(relation_t *reln, uint32_t ntuples, uint32_t maxid, unsigned int rate);

int
create_relation_from_file(relation_t *rel, char* filename, unsigned int rate);

/**
 * Create relation with only primary keys (i.e. keys are unique from 1 to
 * num_tuples) 
 */
int 
create_relation_pk(struct table_t *reln, uint32_t ntuples, unsigned int rate);

/**
 * Create relation with foreign keys (i.e. duplicated keys exist). If ntuples is
 * an exact multiple of maxid, (ntuples/maxid) sub-relations with shuffled keys
 * following each other are generated.
 */
int 
create_relation_fk(struct table_t *reln, uint32_t ntuples, const uint32_t maxid, unsigned int rate);

/**
 * Create a foreign-key relation using the given primary-key relation and
 * foreign-key relation size. If the keys in pkrel is randomly distributed in 
 * the full integer range, then 
 */
int 
create_relation_fk_from_pk(struct table_t *fkrel, struct table_t *pkrel, int64_t ntuples, int sorted);

/**
 * Create relation with keys distributed with zipf between [0, maxid]
 * - zipf_param is the parameter of zipf distr (aka s)
 * - maxid is equivalent to the alphabet size
 */
int
create_relation_zipf(relation_t * reln, uint32_t ntuples,
                     uint32_t maxid, double zipfparam, unsigned int rate);


void *
alloc_aligned(size_t size);
/**
 * Free memory allocated for only tuples.
 */
void 
delete_relation(struct table_t * reln);

void
write_relation(struct table_t * rel, const char * filename);

/** @} */

#endif /* GENERATOR_H */
