//
// File:        ix_indexhandle.cc
// Description: IX_IndexHandle handles manipulations within the index
// Author:      <Your Name Here>
//

#include <unistd.h>
#include <sys/types.h>
#include "ix.h"
#include "ix_internal.h"
#include "pf.h"
#include "comparators.h"
#include <cstdio>
#include <math.h>



IX_IndexHandle::IX_IndexHandle(){
    isOpenHandle = false;       // indexhandle is initially closed
    header_modified = false;
}

IX_IndexHandle::~IX_IndexHandle(){

}

RC IX_IndexHandle::InsertEntry(void *pData, const RID &rid)
{
  // Implement this
}

RC IX_IndexHandle::DeleteEntry(void *pData, const RID &rid)
{
  // Implement this
}

RC IX_IndexHandle::ForcePages()
{
  // Implement this
}


/*
 * This function creates a new page and sets it up as a node. It returns the open
 * PF_PageHandle, the page number, and the pointer to its data.
 * isLeaf is a boolean that signifies whether this page should be a leaf or not
 */
RC IX_IndexHandle::CreateNewNode(PF_PageHandle &ph, PageNum &page, char *&nData, bool isLeaf){
    RC rc = 0;
    if((rc = pfh.AllocatePage(ph)) || (rc = ph.GetPageNum(page))){
        return (rc);
    }
    if((rc = ph.GetData(nData)))
        return (rc);
    struct IX_NodeHeader *nHeader = (struct IX_NodeHeader *)nData;

    nHeader->isLeafNode = isLeaf;
    nHeader->isEmpty = true;
    nHeader->num_keys = 0;
    nHeader->invalid1 = NO_MORE_PAGES;
    nHeader->invalid2 = NO_MORE_PAGES;
    nHeader->firstSlotIndex = NO_MORE_SLOTS;
    nHeader->freeSlotIndex = 0;

    struct Node_Entry *entries = (struct Node_Entry *)((char*)nHeader + header.entryOffset_N);

    for(int i=0; i < header.maxKeys_N; i++){ // Sets up the slot pointers into a
        entries[i].isValid = UNOCCUPIED;       // linked list in the freeSlotIndex list
        entries[i].page = NO_MORE_PAGES;
    }
    for(int i=0; i < header.maxKeys_N; i++){ // Sets up the slot pointers into a
        entries[i].isValid = UNOCCUPIED;       // linked list in the freeSlotIndex list
        entries[i].page = NO_MORE_PAGES;
        if(i == (header.maxKeys_N -1))
            entries[i].nextSlot = NO_MORE_SLOTS;
        else
            entries[i].nextSlot = i+1;
    }
    return (rc);
}

/*
 * Calculates the number of keys in a node that it can hold based on a given
 * attribute length.
 */
int IX_IndexHandle::CalcNumKeysNode(int attrLength){
    int body_size = PF_PAGE_SIZE - sizeof(struct IX_NodeHeader);
    return floor(1.0*body_size / (sizeof(struct Node_Entry) + attrLength));
}


/*
 * This function check that the header is a valid header based on the sizes of the attributes,
 * the number of keys, and the offsets. It returns true if it is, and false if it's not
 */
bool IX_IndexHandle::isValidIndexHeader() const{
    if(header.maxKeys_N <= 0){
        printf("error 1");
        return false;
    }
    if(header.entryOffset_N != sizeof(struct IX_NodeHeader)){
        printf("error 2");
        return false;
    }

    int attrLength2 = (header.keysOffset_N - header.entryOffset_N)/(header.maxKeys_N);
    if(attrLength2 != sizeof(struct Node_Entry)){
        printf("error 3");
        return false;
    }
    if(header.keysOffset_N + header.maxKeys_N * header.attr_length > PF_PAGE_SIZE)
        return false;
    return true;
}


/*
 * This finds the index in a node in which to insert a key into, given the node
 * header and the key to insert. It returns the index to insert into, and whether
 * there already exists a key of this value in this particular node.
 */
RC IX_IndexHandle::FindNodeInsertIndex(struct IX_NodeHeader *nHeader,
                                       void *pData, int& index, bool& isDup){
    // Setup
    struct Node_Entry *entries = (struct Node_Entry *)((char *)nHeader + header.entryOffset_N);
    char *keys = ((char *)nHeader + header.keysOffset_N);

    // Search until we reach a key in the node that is greater than the pData entered
    int prev_idx = BEGINNING_OF_SLOTS;
    int curr_idx = nHeader->firstSlotIndex;
    isDup = false;
    while(curr_idx != NO_MORE_SLOTS){
        char *value = keys + header.attr_length * curr_idx;
        int compared = comparator(pData, (void*) value, header.attr_length);
        if(compared == 0)
            isDup = true;
        if(compared < 0)
            break;
        prev_idx = curr_idx;
        curr_idx = entries[prev_idx].nextSlot;
    }
    index = prev_idx;
    return (0);
}