/*
Copyright 2022 Ivan Carvalho

Use of this source code is governed by an MIT-style
license that can be found in the LICENSE file or at
https://opensource.org/licenses/MIT.
*/
#ifndef APPEND_PGM_H
#define APPEND_PGM_H

#if defined(__cplusplus)
extern "C" {
#endif

#define MAX_PGM_LEVELS 25

#include <math.h>
#include <stdint.h>
#include <stdbool.h>
#include "one_level_append_pgm.h"

typedef struct {
	/* Underlying data */
	uint32_t maxError;						 /* Maximum error              */
	size_t num_levels;						 /* Number of levels in PGM    */
	size_t count;						     /* Number of points in PGM    */
	size_t size;							 /* Maximum number of points   */
    size_t size_second;						 /* Maximum number of points in second level */

	/* Implementation details */
	one_level_pgm** levels; /* One and only level of PGM */
} append_pgm;


append_pgm* appendPGMInit(size_t size, size_t size_second, size_t maxError) {
    /* Basic details */
    append_pgm* pgm = (append_pgm*) malloc(sizeof (append_pgm));
    pgm->size = size;
    pgm->size_second = size_second;
    pgm->maxError = maxError;
    pgm->count = 0;
    pgm->levels = (one_level_pgm**) malloc(sizeof(one_level_pgm*) * MAX_PGM_LEVELS);

    /* First level */
    pgm->num_levels = 0;
    size_t div_factor = 2*pgm->maxError;
    if(div_factor < 1) {
        div_factor = 1;
    }

    size_t level_size = (pgm->size / div_factor) + 1;
    one_level_pgm* new_level = oneLevelPGMInit(level_size, pgm->maxError);
    pgm->levels[pgm->num_levels++] = new_level;

    return pgm;
}

void appendPGMAdd(append_pgm *pgm, pgm_key_t key) {

    pgm->count += 1;

    for(size_t current = 0; current < pgm->num_levels; current++){
        size_t previous_level_size = pgm->levels[current]->level_pos;

        if (current == 0) {
            oneLevelPGMAdd(pgm->levels[current], key);
        }
        else {
            pgm_key_t last_key = pgm->levels[current - 1]->latest_pair.x;
            oneLevelPGMAdd(pgm->levels[current], last_key);
        }

        size_t new_level_size = pgm->levels[current]->level_pos;
        if (new_level_size == previous_level_size) { // no new point, leave upper levels intact
            return;
        }
    }

    if (pgm->levels[pgm->num_levels - 1]->level_pos > 1) {
        size_t div_factor = 2*pgm->maxError;
        if(div_factor < 1) {
            div_factor = 1;
        }

        size_t level_size = (pgm->levels[pgm->num_levels - 1]->size / div_factor) + 1;
        if (pgm->num_levels - 1 == 0) {
            level_size = pgm->size_second;
        }
        one_level_pgm* new_level = oneLevelPGMInit(level_size, pgm->maxError);
        pgm->levels[pgm->num_levels++] = new_level;

        oneLevelPGMAdd(pgm->levels[pgm->num_levels - 1], pgm->levels[pgm->num_levels - 2]->level[0].pos);
        oneLevelPGMAdd(pgm->levels[pgm->num_levels - 1], pgm->levels[pgm->num_levels - 2]->level[1].pos);
    }

}

void appendPGMBuild(append_pgm *pgm, pgm_key_t* keys, size_t size, size_t maxError) {
    for (size_t i = 0; i < size; i++) {
        appendPGMAdd(pgm, keys[i]);
    }
}

pgm_approx_pos appendPGMApproxSearch(append_pgm *pgm, pgm_key_t key) {
    pgm_approx_pos answer;

    if(key < pgm->levels[0]->smallest_key) {
        answer.lo = 1;        
        answer.hi = 0;
        return answer;
    }

    one_level_pgm* level_pgm = pgm->levels[pgm->num_levels - 1];
    size_t maxError = level_pgm->maxError;
    size_t model_index = 0;

    for (size_t current = pgm->num_levels - 1; current >= 0;) {
        size_t level_size = level_pgm->count;
        double pred = level_pgm->level[model_index].a * ((double)key) + level_pgm->level[model_index].b;
        double lo_pred = pred - maxError;
        lo_pred = (lo_pred < 0) ? (0) : (lo_pred);

        double hi_pred = pred + (double)maxError + 1;

        answer.lo = (size_t)lo_pred;
        answer.hi = (size_t)hi_pred;
        answer.lo = (answer.lo <= (level_size - 1)) ? (answer.lo) : (level_size - 1);
        answer.hi = (answer.hi <= (level_size - 1)) ? (answer.hi) : (level_size - 1);

        if (current >= 1) {
            level_pgm = pgm->levels[current - 1];
            for (size_t i = answer.hi; i >= answer.lo;) {
                if (key >= level_pgm->level[i].pos) {
                    model_index = i;
                    break;
                }
                if (i > 0) {
                    i -= 1;
                }
            }
            current -= 1;
        }
        else {
            break;
        }

    }

    return answer;
}

void appendPGMFree(append_pgm *pgm) {
    for(size_t i = 0; i < pgm->num_levels; i++){
        oneLevelPGMFree(pgm->levels[i]);
    }

    free(pgm->levels);
    free(pgm);
}

size_t appendPGMSizeBytes(append_pgm *pgm) {
    size_t level_sizes = 0;
    for(size_t i = 0; i < pgm->num_levels; i++){
        level_sizes += oneLevelPGMSizeBytes(pgm->levels[i]);
    }
    size_t aux_size = sizeof(append_pgm);
    return level_sizes + aux_size;
}

#if defined(__cplusplus)
}
#endif

#endif