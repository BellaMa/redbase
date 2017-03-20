#include <unistd.h>
#include <sys/types.h>
#include "pf.h"
#include <stdlib.h>
#include <cstdio>
#include <cstring>

static int compare_string(void *value1, void* value2, int attrLength){
  return strncmp((char *) value1, (char *) value2, attrLength);
}

static int compare_int(void *value1, void* value2, int attrLength){
  if((*(int *)value1 < *(int *)value2))
    return -1;
  else if((*(int *)value1 > *(int *)value2))
    return 1;
  else
    return 0;
}

static int compare_float(void *value1, void* value2, int attrLength){
  if((*(float *)value1 < *(float *)value2))
    return -1;
  else if((*(float *)value1 > *(float *)value2))
    return 1;
  else
    return 0;
}

static int compare_mbr(MBR &rec1, MBR &rec2, int attrLength){
  /* 0 rec1 overlaps with rec2 but not inside rec2
   * 1 rec1 is inside rec2
   * 2 rec1 is outside rec2 = no intersection
   * 3 rec1 covers rec2
   *
   * If equal, then 1, rec1 is inside rec2
   * */
  if( rec1.left>=rec2.left && rec1.right<=rec2.right && rec1.bottom>=rec2.bottom && rec1.top<=rec2.top)
    return 1;
  else if( rec1.top<=rec2.bottom || rec1.bottom>=rec2.top || rec1.left>=rec2.right || rec1.right<=rec2.left)
    return 2;
  else if(rec1.left<=rec2.left && rec1.right>=rec2.right && rec1.bottom<=rec2.bottom && rec1.top>=rec2.top)
    return 3;
  else
    return 0;
}

static bool print_string(void *value, int attrLength){
  char * str = (char *)malloc(attrLength + 1);
  memcpy(str, value, attrLength+1);
  str[attrLength] = '\0';
  printf("%s ", str);
  free(str);
  return true;
}

static bool print_int(void *value, int attrLength){
  int num = *(int*)value;
  printf("%d ", num);
  return true;
}

static bool print_float(void *value, int attrLength){
  float num = *(float *)value;
  printf("%f ", num);
  return true;
}

static bool print_mbr(void *value, int attrLength){
  MBR rec = *(MBR *) value;
  printf("[%f,%f,%f,%f]", rec.left,rec.right,rec.bottom,rec.top);
  return true;
}