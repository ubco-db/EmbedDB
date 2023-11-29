#include <stdint.h>
#include <stdlib.h>
#include <string.h>

void updateCustomUWABitmap(void *data, void *bm) {
    int32_t temp = *(int32_t *)data;

    /*  Custom, equi-depth buckets
     *  Bucket  0: 315 -> 373 (0, 6249)
     *	Bucket  1: 373 -> 385 (6250, 12499)
     *	Bucket  2: 385 -> 398 (12500, 18749)
     *	Bucket  3: 398 -> 408 (18750, 24999)
     *	Bucket  4: 408 -> 416 (25000, 31249)
     *	Bucket  5: 416 -> 423 (31250, 37499)
     *	Bucket  6: 423 -> 429 (37500, 43749)
     *	Bucket  7: 429 -> 435 (43750, 49999)
     *	Bucket  8: 435 -> 443 (50000, 56249)
     *	Bucket  9: 443 -> 449 (56250, 62499)
     *	Bucket 10: 449 -> 456 (62500, 68749)
     *	Bucket 11: 456 -> 464 (68750, 74999)
     *	Bucket 12: 464 -> 473 (75000, 81249)
     *	Bucket 13: 473 -> 484 (81250, 87499)
     *	Bucket 14: 484 -> 500 (87500, 93749)
     *	Bucket 15: 500 -> 602 (93750, 99999)
     */

    uint16_t mask = 1;

    int8_t mode = 0;  // 0 = equi-depth, 1 = equi-width
    if (mode == 0) {
        if (temp < 373) {
            mask <<= 0;
        } else if (temp < 385) {
            mask <<= 1;
        } else if (temp < 398) {
            mask <<= 2;
        } else if (temp < 408) {
            mask <<= 3;
        } else if (temp < 416) {
            mask <<= 4;
        } else if (temp < 423) {
            mask <<= 5;
        } else if (temp < 429) {
            mask <<= 6;
        } else if (temp < 435) {
            mask <<= 7;
        } else if (temp < 443) {
            mask <<= 8;
        } else if (temp < 449) {
            mask <<= 9;
        } else if (temp < 456) {
            mask <<= 10;
        } else if (temp < 464) {
            mask <<= 11;
        } else if (temp < 473) {
            mask <<= 12;
        } else if (temp < 484) {
            mask <<= 13;
        } else if (temp < 500) {
            mask <<= 14;
        } else {
            mask <<= 15;
        }
    } else {
        int shift = (temp - 303) / 16;
        if (shift < 0) {
            shift = 0;
        } else if (shift > 15) {
            shift = 15;
        }
        mask <<= shift;
    }

    *(uint16_t *)bm |= mask;
}

int8_t inCustomUWABitmap(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateCustomUWABitmap(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return (tmpbm & *bmval) > 0;
}

void buildCustomUWABitmapFromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 65535; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateCustomUWABitmap(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = ~(minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateCustomUWABitmap(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = maxMap | (maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}

void updateCustomEthBitmap(void *data, void *bm) {
    int32_t temp = *(int32_t *)((int8_t *)data + 4);

    uint16_t mask = 1;

    if (temp < 0) {
        mask <<= 0;
    } else if (temp == 0) {
        mask <<= 1;
    } else {
        mask <<= 2;
    }

    *(uint8_t *)bm |= mask;
}

int8_t inCustomEthBitmap(void *data, void *bm) {
    uint8_t *bmval = (uint8_t *)bm;

    uint8_t tmpbm = 0;
    updateCustomEthBitmap(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return (tmpbm & *bmval) > 0;
}

void buildCustomEthBitmapFromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint8_t *)bm = 255; /* Everything */
        return;
    } else {
        uint8_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateCustomEthBitmap(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = (~(minMap - 1)) << 1;
            if (max == NULL) {
                *(uint8_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateCustomEthBitmap(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = maxMap | (maxMap - 1);
            if (min == NULL) {
                *(uint8_t *)bm = maxMap;
                return;
            }
        }
        *(uint8_t *)bm = minMap & maxMap;
    }
}

void updateCustomWatchBitmap(void *data, void *bm) {
    int32_t temp = *(int32_t *)(data);
    float norm = abs(temp) / 1e9;

    uint16_t mask = 1;

    // Divide the 0-1 range in 16 buckets
    if (norm < 0.0625) {
        mask <<= 0;
    } else if (norm < 0.125) {
        mask <<= 1;
    } else if (norm < 0.1875) {
        mask <<= 2;
    } else if (norm < 0.25) {
        mask <<= 3;
    } else if (norm < 0.3125) {
        mask <<= 4;
    } else if (norm < 0.375) {
        mask <<= 5;
    } else if (norm < 0.4375) {
        mask <<= 6;
    } else if (norm < 0.5) {
        mask <<= 7;
    } else if (norm < 0.5625) {
        mask <<= 8;
    } else if (norm < 0.625) {
        mask <<= 9;
    } else if (norm < 0.6875) {
        mask <<= 10;
    } else if (norm < 0.75) {
        mask <<= 11;
    } else if (norm < 0.8125) {
        mask <<= 12;
    } else if (norm < 0.875) {
        mask <<= 13;
    } else if (norm < 0.9375) {
        mask <<= 14;
    } else {
        mask <<= 15;
    }

    *(uint16_t *)bm |= mask;
}

int8_t inCustomWatchBitmap(void *data, void *bm) {
    uint16_t *bmval = (uint16_t *)bm;

    uint16_t tmpbm = 0;
    updateCustomWatchBitmap(data, &tmpbm);

    // Return a number great than 1 if there is an overlap
    return (tmpbm & *bmval) > 0;
}

void buildCustomWatchBitmapFromRange(void *min, void *max, void *bm) {
    if (min == NULL && max == NULL) {
        *(uint16_t *)bm = 255; /* Everything */
        return;
    } else {
        uint16_t minMap = 0, maxMap = 0;
        if (min != NULL) {
            updateCustomWatchBitmap(min, &minMap);
            // Turn on all bits below the bit for min value (cause the lsb are for the higher values)
            minMap = ~(minMap - 1);
            if (max == NULL) {
                *(uint16_t *)bm = minMap;
                return;
            }
        }
        if (max != NULL) {
            updateCustomWatchBitmap(max, &maxMap);
            // Turn on all bits above the bit for max value (cause the msb are for the lower values)
            maxMap = maxMap | (maxMap - 1);
            if (min == NULL) {
                *(uint16_t *)bm = maxMap;
                return;
            }
        }
        *(uint16_t *)bm = minMap & maxMap;
    }
}

int8_t customCol2Int32Comparator(void *a, void *b) {
    int32_t i1, i2;
    memcpy(&i1, (int8_t *)a + 4, sizeof(int32_t));
    memcpy(&i2, (int8_t *)b + 4, sizeof(int32_t));
    int32_t result = i1 - i2;
    if (result < 0)
        return -1;
    if (result > 0)
        return 1;
    return 0;
}
