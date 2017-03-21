//
// Created by Luffy's Mac on 20/03/2017.
//
//Reference from yifeih's work

#ifndef REDBASE_IX_INTERNAL_H
#define REDBASE_IX_INTERNAL_H

#include "ix.h"

#define NO_MORE_PAGES -1
#define NO_MORE_SLOTS -1

// The following structs define the headers for each node.
// IX_NodeHeader is used as a generic cast for all nodes.
// IX_NodeHeader_I is used once we know the node is an internal node
// IX_NodeHeader_L is used once we know the node is a leaf node
struct IX_NodeHeader{
    bool isLeafNode;  // indicator for whether it is a leaf node
    bool isEmpty;     // Whether the node contains pointers or not
    int num_keys;     // number of valid keys the node holds

    // valid key/pointer slots
    PageNum invalid1;
    PageNum invalid2;
};

struct IX_NodeHeader_I{
    bool isLeafNode;
    bool isEmpty;     // whether the node has its first page pointer or not
    int num_keys;

    PageNum firstPage; // first leaf page under this internal node
    PageNum invalid2;
};

struct IX_NodeHeader_L{
    bool isLeafNode;
    bool isEmpty;
    int num_keys;

    PageNum nextPage; // next leaf page
    PageNum prevPage; // previous leaf page
};


// The following are "entries", or the headers that provide
// information about each slot
struct Entry{
    char isValid;
    int nextSlot;
};

struct Node_Entry{
    char isValid;     // Whether the slot is valid, contains a duplicate
    // value, or a single value
    PageNum page;     // Maybe delete
    SlotNum slot;     // Maybe keep as the pointer to record.
    // (only valid for leaf nodes)
};


#endif //REDBASE_IX_INTERNAL_H
