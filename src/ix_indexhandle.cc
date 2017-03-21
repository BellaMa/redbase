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
#include "node_comps.h"
#include <cstdio>
#include <math.h>
#include <limits>
#include <redbase.h>


IX_IndexHandle::IX_IndexHandle(){
    isOpenHandle = false;       // indexhandle is initially closed
    header_modified = false;
}

IX_IndexHandle::~IX_IndexHandle(){

}

/*
 * This function initializes the insert of a certain value and RID into the
 * Index. If the entry already exists, return IX_DUPLICATEENTRY
 */
RC IX_IndexHandle::InsertEntry(void *pData, const RID &rid){
    if(isOpenHandle == false){
    }

    // only insert if this is a valid, open indexHandle
    if(! isValidIndexHeader() || isOpenHandle == false)
        return (IX_INVALIDINDEXHANDLE);

    // Retrieve the root header
    RC rc = 0;
    struct IX_NodeHeader *rHeader;
    if((rc = rootPH.GetData((char *&)rHeader))){
        return (rc);
    }

    // If the root is full, create a new empty root node
    if(rHeader->num_keys == header.maxKeys_N){
        PageNum newRootPage;
        char *newRootData;
        PF_PageHandle newRootPH;
        if((rc = CreateNewNode(newRootPH, newRootPage, newRootData, false))){
            return (rc);
        }
        struct IX_NodeHeader_I *newRootHeader = (struct IX_NodeHeader_I *)newRootData;
        newRootHeader->isEmpty = false;
        newRootHeader->isLeafNode = false;
        newRootHeader->firstPage = header.rootPage; // update the root node

        int unused;
        PageNum unusedPage;
        // Split the current root node into two nodes, and make the parent the new
        // root node
        if((rc = SplitNode((struct IX_NodeHeader *&)newRootData, (struct IX_NodeHeader *&)rHeader, header.rootPage,
                           BEGINNING_OF_SLOTS, unused, unusedPage)))
            return (rc);
        if((rc = pfh.MarkDirty(header.rootPage)) || (rc = pfh.UnpinPage(header.rootPage)))
            return (rc);
        rootPH = newRootPH; // reset root PF_PageHandle
        header.rootPage = newRootPage;
        header_modified = true; // New root page has been set, so the index header has been modified

        // Retrieve the contents of the new Root node
        struct IX_NodeHeader *useMe;
        if((rc = newRootPH.GetData((char *&)useMe))){
            return (rc);
        }
        // Insert into the non-full root node
        if((rc = InsertIntoNonFullNode(useMe, header.rootPage, pData, rid)))
            return (rc);
    }
    else{ // If root is not full, insert into it
        if((rc = InsertIntoNonFullNode(rHeader, header.rootPage, pData, rid))){
            return (rc);
        }
    }

    // Mark the root node as dirty
    if((rc = pfh.MarkDirty(header.rootPage)))
        return (rc);

    return (rc);
}

/*
 * Given a key and a RID, delete that entry in the index. If it
 * does not exist, return IX_INVALIDENTRY.
 */
RC IX_IndexHandle::DeleteEntry(void *pData, const RID &rid){
    RC rc = 0;
    if(! isValidIndexHeader() || isOpenHandle == false)
        return (IX_INVALIDINDEXHANDLE);

    // get root page
    struct IX_NodeHeader *rHeader;
    if((rc = rootPH.GetData((char *&)rHeader))){
        printf("failing here\n");
        return (rc);
    }

    // If the root page is empty, then no entries can exist
    if(rHeader->isEmpty && (! rHeader->isLeafNode) )
        return (IX_INVALIDENTRY);
    if(rHeader->num_keys== 0 && rHeader->isLeafNode)
        return (IX_INVALIDENTRY);

    // toDelete is an indicator for whether to delete this current node
    // because it has no more contents
    bool toDelete = false;
    bool RecIn = false;
    if((rc = DeleteFromNode(rHeader, pData, rid, toDelete, RecIn, NULL))) // Delete the value from this node
        return (rc);

    // If the tree is empty, set the current node to a leaf node.
    if(toDelete){
        rHeader->isLeafNode = true;
    }

    return (rc);
}

/*
 * Forces all pages in the index only if the indexHandle refers to a valid open index
 */
RC IX_IndexHandle::ForcePages(){
    RC rc = 0;
    if (isOpenHandle == false)
        return (IX_INVALIDINDEXHANDLE);
    pfh.ForcePages();
    return (rc);
}


RC IX_IndexHandle::PrintIndex(){
    RC rc;
    PageNum leafPage;
    PF_PageHandle ph;
    struct IX_NodeHeader_L *lheader;
    struct Node_Entry *entries;
    char *keys;
    return (0);
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

    struct Node_Entry *entries = (struct Node_Entry *)((char*)nHeader + header.entryOffset_N);

    for(int i=0; i < header.maxKeys_N; i++){ // Sets up the slot pointers into a
        entries[i].isValid = UNOCCUPIED;       // linked list in the freeSlotIndex list
        entries[i].page = NO_MORE_PAGES;
    }
    for(int i=0; i < header.maxKeys_N; i++){ // Sets up the slot pointers into a
        entries[i].isValid = UNOCCUPIED;       // linked list in the freeSlotIndex list
        entries[i].page = NO_MORE_PAGES;
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
 * This function deletes an entry from a leaf given the header of the leaf. It returns
 * in toDelete whether this leaf node is empty, and whether to delete it
 */
RC IX_IndexHandle::DeleteFromLeaf(struct IX_NodeHeader_L *nHeader, void *pData, const RID &rid, bool &toDelete, bool &RecIn, void* parentKey){
    RC rc = 0;
    struct Node_Entry *entries = (struct Node_Entry *) ((char *)nHeader + header.entryOffset_N);
    char *keys = (char *)nHeader + header.keysOffset_N;
    for(int i=0;i<header.maxKeys_N;++i){
        if(entries[i].isValid==UNOCCUPIED)
            continue;
        if(!compare_string(keys+i*header.attr_length, pData, header.attr_length))
            continue;
        PageNum ridPage;
        SlotNum ridSlot;
        if((rc = rid.GetPageNum(ridPage))|| (rc = rid.GetSlotNum(ridSlot)))
            return (rc);
        if(ridPage == entries[i].page && ridSlot == entries[i].slot){
            RecIn = true;
            --nHeader->num_keys;
            entries[i].isValid = UNOCCUPIED;
        }
    }
    if(nHeader->num_keys == 0){ // If the leaf is now empty,
        toDelete = true;          // return the indicator to delete
    }
    else{
        MBR newMBR;
        newMBR.left = numeric_limits<float>::max();
        newMBR.right = numeric_limits<float>::min();
        newMBR.bottom = numeric_limits<float>::max();
        newMBR.top = numeric_limits<float>::min();
        memcpy(parentKey, &newMBR, sizeof(MBR));
        for(int i=0;i<header.maxKeys_N;i++){
            if(entries[i].isValid==UNOCCUPIED)
                continue;
            mbr_update((char *) parentKey, parentKey, keys+i*header.attr_length);
        }
    }
    return (0);
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
 * This inserts a value and RID into a node given its header and page number.
 */
RC IX_IndexHandle::InsertIntoNonFullNode(struct IX_NodeHeader *nHeader, PageNum thisNodeNum, void *pData,
                                         const RID &rid){
    RC rc = 0;

    // Retrieve contents of this node
    struct Node_Entry *entries = (struct Node_Entry *) ((char *)nHeader + header.entryOffset_N);
    char *keys = (char *)nHeader + header.keysOffset_N;

    // If it is a leaf node, then insert into it
    if(nHeader->isLeafNode){
        //find the index that still empty
        int index;
        for(int i=0;i<header.maxKeys_N;++i){
            if(entries[i].isValid == UNOCCUPIED){
                entries[i].isValid = OCCUPIED_NEW;
                if((rc = rid.GetPageNum(entries[i].page)) || (rc = rid.GetSlotNum(entries[i].slot)))
                    return (rc);
                memcpy(keys + header.attr_length * i, (char *)pData, header.attr_length);
                nHeader->isEmpty = false;
                ++nHeader->num_keys;
                break;
            }
        }
    }
    else{ // Otherwise, this is a internal node
        // Get its contents, and find the insert location
        struct IX_NodeHeader_I *nIHeader = (struct IX_NodeHeader_I *)nHeader;
        PageNum nextNodePage;
        int prevInsertIndex = BEGINNING_OF_SLOTS;
        if((rc = FindNodeInsertIndex(nHeader, pData, prevInsertIndex)))
            return (rc);
        if(prevInsertIndex == BEGINNING_OF_SLOTS)
            nextNodePage = nIHeader->firstPage;
        else{
            nextNodePage = entries[prevInsertIndex].page;
        }

        // Read this next page to insert into.
        PF_PageHandle nextNodePH;
        struct IX_NodeHeader *nextNodeHeader;
        int newKeyIndex;
        int nextKeyIndex = prevInsertIndex;
        PageNum newPageNum;
        if((rc = pfh.GetThisPage(nextNodePage, nextNodePH)) || (rc = nextNodePH.GetData((char *&)nextNodeHeader)))
            return (rc);
        // If this next node is full, the split the node
        if(nextNodeHeader->num_keys == header.maxKeys_N){
            if((rc = SplitNode(nHeader, nextNodeHeader, nextNodePage, prevInsertIndex, newKeyIndex, newPageNum)))
                return (rc);
            char *value1 = keys + newKeyIndex*header.attr_length;
            char *value2 = keys + prevInsertIndex*header.attr_length;
            // check which of the two split nodes to insert into.
            float diff1 = mbr_change(pData, (void*) value1);
            float diff2 = mbr_change(pData, (void*) value2);
            if(diff1 > diff2){
                //insert into the new node
                PageNum nextPage = newPageNum;
                if((rc = pfh.MarkDirty(nextNodePage)) || (rc = pfh.UnpinPage(nextNodePage)))
                    return (rc);
                if((rc = pfh.GetThisPage(nextPage, nextNodePH)) || (rc = nextNodePH.GetData((char *&) nextNodeHeader)))
                    return (rc);
                nextNodePage = nextPage;
                nextKeyIndex = newKeyIndex;
            }
        }
        // Insert into the following node, then mark it dirty and unpin it
        if((rc = InsertIntoNonFullNode(nextNodeHeader, nextNodePage, pData, rid)))
            return (rc);
        mbr_update(keys+nextKeyIndex*header.attr_length, keys+nextKeyIndex*header.attr_length, pData);
        if((rc = pfh.MarkDirty(nextNodePage)) || (rc = pfh.UnpinPage(nextNodePage)))
            return (rc);
    }
    return (rc);
}


/*
 * This finds the index in a node in which to insert a key into, given the node
 * header and the key to insert. It returns the index to insert into, and whether
 * there already exists a key of this value in this particular node.
 */
RC IX_IndexHandle::FindNodeInsertIndex(struct IX_NodeHeader *nHeader,
                                       void *pData, int& index){
    // Setup
    struct Node_Entry *entries = (struct Node_Entry *)((char *)nHeader + header.entryOffset_N);
    char *keys = ((char *)nHeader + header.keysOffset_N);

    // Search until we reach a key in the node that is greater than the pData entered

    float difference = std::numeric_limits<float>::max();

    //Search for the branch that has minimum change to cover pData
    for(int curr_idx = 0; curr_idx<header.maxKeys_N; ++curr_idx){
        if (entries[curr_idx].isValid==UNOCCUPIED)
            continue;
        char *value = keys + header.attr_length * curr_idx;
        float tmpDifference = mbr_change(pData, (void*) value);
        if(tmpDifference==0){
            index = curr_idx;
            return (0);
        }
        else if(tmpDifference<difference){
            difference = tmpDifference;
            index = curr_idx;
        }
    }
    return (0);
}


/*
 * This function deletes a entry RID/key from a node given its node Header. It returns
 * a boolean toDelete that indicates whether the current node is empty or not, to signal
 * to the caller to delete this node
 */
RC IX_IndexHandle::DeleteFromNode(struct IX_NodeHeader *nHeader, void *pData, const RID &rid, bool &toDelete, bool &RecIn, void *parentKey){
    RC rc = 0;
    toDelete = false;
    struct Node_Entry *entries = (struct Node_Entry *) ((char *)nHeader + header.entryOffset_N);
    char *keys = (char *)nHeader + header.keysOffset_N;
    for(int i=0;i<header.maxKeys_N;++i){
        if(entries[i].isValid==UNOCCUPIED)
            continue;
        if(nequal(pData, keys+i*header.attr_length, _MBR, header.attr_length)){
            PageNum nextNodePage = entries[i].page;
            PF_PageHandle nextNodePH;
            struct IX_NodeHeader *nextNodeHeader;
            if((rc = pfh.GetThisPage(nextNodePage, nextNodePH)) || (rc = nextNodePH.GetData((char *&)nextNodeHeader)))
                return (rc);
            if(rc = DeleteFromNode( nextNodeHeader, pData, rid, toDelete, RecIn, keys+i*header.attr_length))
                return(rc);
            if(toDelete){
                entries[i].isValid = UNOCCUPIED;
                --nHeader->num_keys;
                if(nHeader->num_keys == 0){ // If the leaf is now empty,
                    toDelete = true;          // return the indicator to delete
                }
            }
            if(RecIn){
                if(parentKey!=NULL){
                    MBR newMBR;
                    newMBR.left = numeric_limits<float>::max();
                    newMBR.right = numeric_limits<float>::min();
                    newMBR.bottom = numeric_limits<float>::max();
                    newMBR.top = numeric_limits<float>::min();
                    memcpy(parentKey, &newMBR, sizeof(MBR));
                    for(int i=0;i<header.maxKeys_N;i++){
                        if(entries[i].isValid==UNOCCUPIED)
                            continue;
                        mbr_update((char *) parentKey, parentKey, keys+i*header.attr_length);
                    }
                }
                return (0);
            }
        }
    }
    return (rc);
}

/*
 * This function deals with splitting a node:
 * pHeader - the header of the parent node
 * oldHeader - the header of the full node to be split
 * oldPage - the PageNum of the old node to be split
 * index - the index into which to insert the new node into in the parent node
 * newKeyIndex - the index of the first key that points to the new node
 * newPageNum - the page number of the new node
 */
RC IX_IndexHandle::SplitNode(struct IX_NodeHeader *pHeader, struct IX_NodeHeader *oldHeader,
                             PageNum oldPage, int index, int & newKeyIndex, PageNum &newPageNum){
    RC rc = 0;
    //printf("********* SPLIT ********* at index %d \n", index);
    bool isLeaf = false;  // Determines if the new page should be a leaf page
    if(oldHeader->isLeafNode == true){
        isLeaf = true;
    }
    PageNum newPage;  // Creates the new page, and acquires its headers
    struct IX_NodeHeader *newHeader;
    PF_PageHandle newPH;
    if((rc = CreateNewNode(newPH, newPage, (char *&)newHeader, isLeaf))){
        return (rc);
    }
    newPageNum = newPage; // returns new page number

    // Retrieve the appropriate pointers to all the nodes' contents
    struct Node_Entry *pEntries = (struct Node_Entry *) ((char *)pHeader + header.entryOffset_N);
    struct Node_Entry *oldEntries = (struct Node_Entry *) ((char *)oldHeader + header.entryOffset_N);
    struct Node_Entry *newEntries = (struct Node_Entry *) ((char *)newHeader + header.entryOffset_N);
    char *pKeys = (char *)pHeader + header.keysOffset_N;
    char *newKeys = (char *)newHeader + header.keysOffset_N;
    char *oldKeys = (char *)oldHeader + header.keysOffset_N;

    // Keep the first header.masKeys_N/2 values in the old node
    for(int i=0; i<header.maxKeys_N/2;i++){
        memcpy(newEntries+i* sizeof(Node_Entry), oldEntries+(i+header.maxKeys_N/2)* sizeof(Node_Entry), sizeof(Node_Entry));
        oldEntries[i].isValid = UNOCCUPIED;
        memcpy(newKeys+i*header.attr_length, oldKeys+(i+header.maxKeys_N/2)*header.attr_length, header.attr_length);
    }
    //Find a Unocupied index to store the new node, which is newKeyIndex
    for(int i=0;i<header.maxKeys_N;++i){
        if(pEntries[i].isValid==UNOCCUPIED){
            newKeyIndex = i;
            pEntries[i].isValid=OCCUPIED_NEW;
            pEntries[i].page = newPageNum;
            break;
        }
    }
    //update the new mbr for the old node
    MBR newMBR;
    newMBR.left = numeric_limits<float>::max();
    newMBR.right = numeric_limits<float>::min();
    newMBR.bottom = numeric_limits<float>::max();
    newMBR.top = numeric_limits<float>::min();
    memcpy(pKeys+index*header.attr_length, &newMBR, sizeof(MBR));
    for(int i=0;i<header.maxKeys_N;i++){
        if(oldEntries[i].isValid==UNOCCUPIED)
            continue;
        mbr_update(pKeys+index*header.attr_length, pKeys+index*header.attr_length, oldKeys+i*header.attr_length);
    }
    //update the new mbr for the new node
    newMBR.left = numeric_limits<float>::max();
    newMBR.right = numeric_limits<float>::min();
    newMBR.bottom = numeric_limits<float>::max();
    newMBR.top = numeric_limits<float>::min();
    memcpy(pKeys+newKeyIndex*header.attr_length, &newMBR, sizeof(MBR));
    for(int i=0;i<header.maxKeys_N;i++){
        if(newEntries[i].isValid==UNOCCUPIED)
            continue;
        mbr_update(pKeys+newKeyIndex*header.attr_length, pKeys+newKeyIndex*header.attr_length, newKeys+i*header.attr_length);
    }
    pHeader->num_keys++;

    // Mark the new page as dirty, and unpin it
    if((rc = pfh.MarkDirty(newPage))||(rc = pfh.UnpinPage(newPage))){
        return (rc);
    }
    return (rc);
}