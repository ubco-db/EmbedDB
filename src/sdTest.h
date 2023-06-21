#include "sbits/sbits.h"

/*
 * Flag values for open(2) and fcntl(2)
 * The kernel adds 1 to the open modes to turn it into some
 * combination of FREAD and FWRITE.
 */
#define O_RDONLY 0 /* +1 == FREAD */
#define O_WRONLY 1 /* +1 == FWRITE */
#define O_RDWR 2   /* +1 == FREAD|FWRITE */
#define O_APPEND 0x0008
#define O_CREAT 0x0200
#define O_TRUNC 0x0400
#define O_EXCL 0x0800
#define O_SYNC 0x2000
#define O_ACCMODE (O_RDONLY | O_WRONLY | O_RDWR)

void test() {
    printf("Starting...\n");

    // int data[] = {0, 1, 2, 3, 4, 5, 6, 7};
    // int readData[] = {0, 0, 0, 0, 0, 0, 0, 0};

    SD_FILE* fp = sd_fopen("testkijfsofs.bin", "r+");
    if (fp == NULL) {
        printf("NULL\n");
    }
    printf("running\n");
    // sd_fwrite(data, sizeof(int), 8, fp);
    // sd_fclose(fp);

    // printf("Starting...\n");

    // fp = sd_fopen_t("test.bin", O_RDWR);
    // sd_fread(readData, sizeof(int), 8, fp);
    // printf("Starting...\n");
    // for (int i = 0; i < 8; i++) {
    //     printf("%d, ", readData[i]);
    // }
    // printf("\n");
    // sd_fclose(fp);
}
