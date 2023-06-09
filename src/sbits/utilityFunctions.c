/**
 * @file utilityFunctions.h
 * @author Ramon Lawernce
 * @brief This file contains some utility functions to be used with sbits.
 * These include functions required to use the bitmap option, and a
 * comparator for comparing keys. They can be modified or implemented
 * differently depending on the application.
 */

#include "utilityFunctions.h"

/* A bitmap with 8 buckets (bits). Range 0 to 100. */
void updateBitmapInt8Bucket(void *data, void *bm) {
    // Note: Assuming int key is right at the start of the data record
    int32_t val = *((int16_t *)data);
    uint8_t *bmval = (uint8_t *)bm;

    if (val < 10)
        *bmval = *bmval | 128;
    else if (val < 20)
        *bmval = *bmval | 64;
    else if (val < 30)
        *bmval = *bmval | 32;
    else if (val < 40)
        *bmval = *bmval | 16;
    else if (val < 50)
        *bmval = *bmval | 8;
    else if (val < 60)
        *bmval = *bmval | 4;
    else if (val < 100)
        *bmval = *bmval | 2;
    else
        *bmval = *bmval | 1;
}

/* A bitmap with 8 buckets (bits). Range 0 to 100. Build bitmap based on min and
 * max value. */
void buildBitmapInt8BucketWithRange(void *min, void *max, void *bm) {
    /* Note: Assuming int key is right at the start of the data record */
    uint8_t *bmval = (uint8_t *)bm;

    if (min == NULL && max == NULL) {
        *bmval = 255; /* Everything */
    } else {
        int8_t i = 0;
        uint8_t val = 128;
        if (min != NULL) {
            /* Set bits based on min value */
            updateBitmapInt8Bucket(min, bm);

            /* Assume here that bits are set in increasing order based on
             * smallest value */
            /* Find first set bit */
            while ((val & *bmval) == 0 && i < 8) {
                i++;
                val = val / 2;
            }
            val = val / 2;
            i++;
        }
        if (max != NULL) {
            /* Set bits based on min value */
            updateBitmapInt8Bucket(max, bm);

            while ((val & *bmval) == 0 && i < 8) {
                i++;
                *bmval = *bmval + val;
                val = val / 2;
            }
        } else {
            while (i < 8) {
                i++;
                *bmval = *bmval + val;
                val = val / 2;
            }
        }
    }
}

int8_t inBitmapInt8Bucket(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateBitmapInt8Bucket(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 16-bit bitmap on a 32-bit int value */
void updateBitmapInt16(void *data, void *bm) {
    int32_t val = *((int32_t *)data);
    uint16_t *bmval = (uint16_t *)bm;

    /* Using a demo range of 0 to 100 */
    // int16_t stepSize = 100 / 15;
    int16_t stepSize = 450 / 15;  // Temperature data in F. Scaled by 10. */
    int16_t minBase = 320;
    int32_t current = minBase;
    uint16_t num = 32768;
    while (val > current) {
        current += stepSize;
        num = num / 2;
    }
    if (num == 0)
        num = 1; /* Always set last bit if value bigger than largest cutoff */
    *bmval = *bmval | num;
}

int8_t inBitmapInt16(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateBitmapInt16(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

/* A 64-bit bitmap on a 32-bit int value */
void updateBitmapInt64(void *data, void *bm) {
    int32_t val = *((int32_t *)data);

    int16_t stepSize = 10;  // Temperature data in F. Scaled by 10. */
    int32_t current = 320;
    int8_t bmsize = 63;
    int8_t count = 0;

    while (val > current && count < bmsize) {
        current += stepSize;
        count++;
    }
    uint8_t b = 128;
    int8_t offset = count / 8;
    b = b >> (count & 7);

    *((char *)((char *)bm + offset)) = *((char *)((char *)bm + offset)) | b;
}

int8_t inBitmapInt64(void *data, void *bm) {
    uint64_t *bmval = (uint64_t *)bm;

    uint64_t tmpbm = 0;
    updateBitmapInt64(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return tmpbm & *bmval;
}

int8_t int32Comparator(void *a, void *b) {
    int32_t i1, i2;
    memcpy(&i1, a, sizeof(int32_t));
    memcpy(&i2, b, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}
