#include <unistd.h>
#include <sys/types.h>
#include "pf.h"
#include <stdlib.h>
#include <cstdio>
#include "comparators.h"
/*
 * The following functions are comparison functions that return 0 if 
 * the two objects are equal, <0 if the first value is smaller, and
 * >0 if the second value is smaller.
 * They must take in an attribute type and attribute length, which 
 * determines the basis to compare the values on.
 */
bool nequal(void * value1, void * value2, AttrType attrtype, int attrLength){
  switch(attrtype){
    case FLOAT: return (*(float *)value1 == *(float*)value2);
    case INT: return (*(int *)value1 == *(int *)value2) ;
    case _MBR: return (compare_mbr(*(MBR *) value1, *(MBR *) value2, attrLength)==2) ;
    default:
      return (strncmp((char *) value1, (char *) value2, attrLength) == 0); 
  }
}

bool nless_than(void * value1, void * value2, AttrType attrtype, int attrLength){
  switch(attrtype){
    case FLOAT: return (*(float *)value1 < *(float*)value2);
    case INT: return (*(int *)value1 < *(int *)value2) ;
    case _MBR: return (compare_mbr(*(MBR *) value1, *(MBR *) value2, attrLength)==1) ;
    default:
      return (strncmp((char *) value1, (char *) value2, attrLength) < 0);
  }
}

bool ngreater_than(void * value1, void * value2, AttrType attrtype, int attrLength){
  switch(attrtype){
    case FLOAT: return (*(float *)value1 > *(float*)value2);
    case INT: return (*(int *)value1 > *(int *)value2) ;
    case _MBR: return (compare_mbr(*(MBR *) value1, *(MBR *) value2, attrLength)==3) ;
    default:
      return (strncmp((char *) value1, (char *) value2, attrLength) > 0);
  }
}

bool nless_than_or_eq_to(void * value1, void * value2, AttrType attrtype, int attrLength){
  int compMBR;
  switch(attrtype){
    case FLOAT: return (*(float *)value1 <= *(float*)value2);
    case INT: return (*(int *)value1 <= *(int *)value2) ;
    case _MBR:
      compMBR = compare_mbr(*(MBR *) value1, *(MBR *) value2, attrLength);
      return (compMBR==1||compMBR==0);
    default: 
      return (strncmp((char *) value1, (char *) value2, attrLength) <= 0);
  }
}

bool ngreater_than_or_eq_to(void * value1, void * value2, AttrType attrtype, int attrLength){
    int compMBR;
  switch(attrtype){
    case FLOAT: return (*(float *)value1 >= *(float*)value2);
    case INT: return (*(int *)value1 >= *(int *)value2) ;
    case _MBR:
        compMBR = compare_mbr(*(MBR *) value1, *(MBR *) value2, attrLength);
          return (compMBR==0||compMBR==3);
    default:
      return (strncmp((char *) value1, (char *) value2, attrLength) >= 0);
  }
}

bool nnot_equal(void * value1, void * value2, AttrType attrtype, int attrLength){
  switch(attrtype){
    case FLOAT: return (*(float *)value1 != *(float*)value2);
    case INT: return (*(int *)value1 != *(int *)value2) ;
    case _MBR: return (compare_mbr(*(MBR *) value1, *(MBR *) value2, attrLength)!=2) ;
    default:
      return (strncmp((char *) value1, (char *) value2, attrLength) != 0);
  }
}