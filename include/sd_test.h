
#if !defined(SD_TEST_H_)
#define SD_TEST_H_

#include "SdFat.h"
#include "sdios.h"

extern cid_t m_cid;
extern csd_t m_csd;
extern uint32_t m_eraseSize;
extern uint32_t m_ocr;


void errorPrint(SdFat32 sd);
void printCardType(SdFat32 sd);
bool cidDmp();
bool mbrDmp(SdFat32 sd);
bool csdDmp();
void dmpVol(SdFat32 sd);

#endif