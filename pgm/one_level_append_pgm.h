/*
Copyright 2022 Ivan Carvalho

Use of this source code is governed by an MIT-style
license that can be found in the LICENSE file or at
https://opensource.org/licenses/MIT.
*/
#ifndef ONE_LEVEL_APPEND_PGM_H
#define ONE_LEVEL_APPEND_PGM_H

#if defined(__cplusplus)
extern "C" {
#endif

#include <math.h>
#include <stdint.h>
#include <stdbool.h>

/* Define type for keys and location ids. */
typedef uint32_t pgm_key_t;
//typedef uint32_t size_t;

typedef struct {
	pgm_key_t x;
	size_t y;
} point_pair;

// Stores line ax + b starting at x = POS
typedef struct {
	double a;
	double b;
	pgm_key_t pos;
} line_segment;

typedef struct {
	size_t lo;
	size_t hi;
} pgm_approx_pos;

typedef struct {
	/* Underlying data */
	uint32_t maxError;						   /* Maximum error              */	
	size_t count;							  /* Number of points in spline */
	size_t size;							 /* Maximum number of points   */

	/* Implementation details */
	line_segment* level; /* One and only level of PGM */
    size_t level_pos;

	pgm_key_t smallest_key;
	pgm_key_t largest_key;
	point_pair latest_pair;
	double upper_a, lower_a;

} one_level_pgm;


one_level_pgm* oneLevelPGMInit(size_t size, size_t maxError) {
    /* Basic details */
    one_level_pgm* pgm = (one_level_pgm*) malloc(sizeof (one_level_pgm));
    pgm->size = size;
    pgm->maxError = maxError;
    pgm->count = 0;

    /* Allocate memory for PGM level */
    size_t div_factor = 2*maxError;
    if(div_factor < 1) {
        div_factor = 1;
    }

    size_t level_size = (size / div_factor) + 1;
    pgm->level = (line_segment*) malloc(sizeof (line_segment) * level_size);
    pgm->level_pos = 0;

    return pgm;
}

void oneLevelPGMAdd(one_level_pgm *pgm, pgm_key_t key) {
    if (pgm->count >= 2) {

        double y_val = (double)pgm->count;

        double upper_prediction = (pgm->upper_a)*((double)key - (double)(pgm->latest_pair.x)) + (double)(pgm->latest_pair.y);

        double lower_prediction = (pgm->lower_a)*((double)key - (double)(pgm->latest_pair.x)) + (double)(pgm->latest_pair.y);

        /* Line 7 from Algorithm 1: Swing Filter  */
        if (
            (y_val - upper_prediction) > (double)pgm->maxError 
            || (y_val - lower_prediction) < -(double)pgm->maxError
        ) { // Add record
            pgm->latest_pair.x = pgm->largest_key;
            pgm->latest_pair.y = pgm->count - 1;

            double dy = (double)1;
            double dx = ((double)key) - ((double)pgm->latest_pair.x);

            line_segment seg;
            seg.a = dy/dx;
            seg.b = ((double)pgm->latest_pair.y) - ((double)pgm->latest_pair.x)*(seg.a);
            seg.pos = pgm->latest_pair.x;

            pgm->level[pgm->level_pos++] = seg;

            /* Calculate coefficient of upper segment */
            pgm->upper_a = ((double)(1.0 + (double)pgm->maxError))/dx;

            /* Calculate coefficient of lower segment */
            pgm->lower_a = ((double)(1.0 - (double)pgm->maxError))/dx;
        }
        else { /* Line 13 from Algorithm 1: Swing Filter  */
            // Line 15 from Algorithm 1: Swing Filter
            if ((y_val - lower_prediction) > (double)pgm->maxError) {
                double dy = (double)(pgm->count - pgm->latest_pair.y);
                double dx = ((double)key) - ((double)pgm->latest_pair.x);
                pgm->lower_a = ((double)(dy - (double)pgm->maxError))/dx;
            }

            // Line 17 from Algorithm 1: Swing Filter
            if ((y_val - upper_prediction) < -(double)pgm->maxError) {
                double dy = (double)(pgm->count - pgm->latest_pair.y);
                double dx = ((double)key) - ((double)pgm->latest_pair.x);
                pgm->upper_a = ((double)(dy + (double)pgm->maxError))/dx;
            }

            // This diverges from Swinger algorithm
            // as we don't calculate the A with Mean-Square error, just the average
            size_t level_last = pgm->level_pos - 1;
            pgm->level[level_last].a = (pgm->upper_a + pgm->lower_a)/2.0;
            pgm->level[level_last].b = ((double)pgm->latest_pair.y) - ((double)pgm->latest_pair.x)*((pgm->upper_a + pgm->lower_a)/2.0);
        }

    }
    else if(pgm->count == 1) { // Add first record
        double dy = (double)1.0;
        double dx = ((double)key) - ((double)pgm->latest_pair.x);

        line_segment seg;
        seg.a = dy/dx;
        seg.b = ((double)pgm->latest_pair.y) - ((double)pgm->latest_pair.x)*(seg.a);
        seg.pos = pgm->latest_pair.x;

        pgm->level[pgm->level_pos++] = seg;

        /* Calculate coefficient of upper segment */
        pgm->upper_a = ((double)(1.0 + (double)pgm->maxError))/dx;

        /* Calculate coefficient of lower segment */
        pgm->lower_a = ((double)(1.0 - (double)pgm->maxError))/dx;
    }
    else {
        /* Level Setup */
        pgm->smallest_key = key;
        pgm->latest_pair.x = key;
        pgm->latest_pair.y = pgm->count;
    }

    /* Add key-value pair */
    pgm->largest_key = key;
    /* Increment for next item */
    pgm->count += 1;
}

void oneLevelPGMBuild(one_level_pgm *pgm, pgm_key_t* keys, size_t size, size_t maxError) {
    for (size_t i = 0; i < size; i++) {
        oneLevelPGMAdd(pgm, keys[i]);
    }
}

pgm_approx_pos oneLevelPGMApproxSearch(one_level_pgm *pgm, pgm_key_t key) {
    pgm_approx_pos answer;

    if(key < pgm->smallest_key) {
        answer.lo = 1;
        answer.hi = 0;
        return answer;
    }

    size_t window = pgm->level_pos;
    size_t offset = 0;
    while (window > 1) {
      size_t half = window >> 1;
      offset += (pgm->level[offset + half].pos < key) ? half : 0;
      window -= half;
    }

    double pred = pgm->level[offset].a * ((double)key) + pgm->level[offset].b;
    double lo_pred = pred - pgm->maxError;
    lo_pred = (lo_pred < 0) ? (0) : (lo_pred);

    double hi_pred = pred + (double)pgm->maxError + 1;

    answer.lo = (size_t)lo_pred;
    answer.hi = (size_t)hi_pred;
    answer.hi = (answer.hi <= (pgm->count - 1)) ? (answer.hi) : (pgm->count - 1);

    return answer;
}

void oneLevelPGMFree(one_level_pgm *pgm) {
    free(pgm->level);
    free(pgm);
}

size_t oneLevelPGMSizeBytes(one_level_pgm *pgm) {
    size_t spline_size =  pgm->level_pos * sizeof(line_segment);
    size_t aux_size = sizeof(one_level_pgm);
    return spline_size + aux_size;
}

#if defined(__cplusplus)
}
#endif

#endif