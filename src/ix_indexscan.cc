//
// File:        ix_indexscan.cc
// Description: IX_IndexHandle handles scanning through the index for a 
//              certain value.
// Author:      Yifei Huang (yifei@stanford.edu)
//

#include <unistd.h>
#include <sys/types.h>
#include "pf.h"
#include "ix_internal.h"
#include "node_comps.h"
#include <cstdio>



IX_IndexScan::IX_IndexScan(){
    openScan = false;  // Initialize all values
    value = NULL;
    initializedValue = false;
    hasBucketPinned = false;
    hasLeafPinned = false;
    scanEnded = true;
    scanStarted = false;
    endOfIndexReached = true;
    attrLength = 0;
    attrType = INT;

    foundFirstValue = false;
    foundLastValue = false;
}

IX_IndexScan::~IX_IndexScan(){
    if(scanEnded == false && hasBucketPinned == true)
        indexHandle->pfh.UnpinPage(currBucketNum);
    if(scanEnded == false && hasLeafPinned == true &&(currLeafNum != (indexHandle->header).rootPage))
        indexHandle->pfh.UnpinPage(currLeafNum);
    if(initializedValue == true){
        free(value);
        initializedValue = false;
    }

}


RC IX_IndexScan::OpenScan(const IX_IndexHandle &indexHandle,
                          CompOp compOp,
                          void *value,
                          ClientHint  pinHint){
    RC rc = 0;

    if(openScan == true || compOp == NE_OP) // makes sure that the scan is not already open
        return (IX_INVALIDSCAN);              // and disallows NE_OP comparator

    if(indexHandle.isValidIndexHeader()) // makes sure that the indexHanlde is valid
        this->indexHandle = const_cast<IX_IndexHandle*>(&indexHandle);
    else
        return (IX_INVALIDSCAN);

    this->value = NULL;
    this->compOp = compOp; // sets up the comparator values, and the comparator
    switch(compOp){
        case EQ_OP : comparator = &nequal;  break;
        case LT_OP : comparator = &nless_than; break;
        case GT_OP : comparator = &ngreater_than;  break;
        case LE_OP : comparator = &nless_than_or_eq_to; break;
        case GE_OP : comparator = &ngreater_than_or_eq_to;  break;
        case NO_OP : comparator = NULL; break;
        default: return (IX_INVALIDSCAN);
    }

    // sets up attribute length and type
    this->attrType = (indexHandle.header).attr_type;
    attrLength = ((this->indexHandle)->header).attr_length;
    if(compOp != NO_OP){
        this->value = (void *) malloc(attrLength); // sets up the value
        memcpy(this->value, value, attrLength);
        initializedValue = true;
    }

    openScan = true; // sets up all indicators
    scanEnded = false;
    hasLeafPinned = false;
    scanStarted = false;
    endOfIndexReached = false;
    foundFirstValue = false;
    foundLastValue = false;

    return (rc);
}

RC IX_IndexScan::BeginScan(PF_PageHandle &leafPH, PageNum &pageNum){
    RC rc = 0;
    if((rc = indexHandle->FindRecordPage(leafPH, pageNum, value)))
        return (rc);
    if((rc = GetAppropriateEntryInLeaf(currLeafPH))){
        if(rc == IX_EOF){
            scanEnded = true;
        }
        return (rc);
    }
    return (rc);
}

/*
 * This function returns the next RID that meets the requirements of the scan
 */
RC IX_IndexScan::GetNextEntry(RID &rid){
    RC rc = 0;
    if(scanEnded == true && openScan == true) // return end of file if the scan has ended
        return (IX_EOF);
    if(foundLastValue == true)
        return (IX_EOF);

    if(scanEnded == true || openScan == false) // if the scan is false, then return IX_INVALIDSCAN
        return (IX_INVALIDSCAN);

    bool notFound = true; // indicator for whether the next value has been found
    while(notFound){
        // In the first iteration of the scan, retrieve the first entry
        if(scanEnded == false && openScan == true && scanStarted == false){
            //if((rc = indexHandle->GetFirstLeafPage(currLeafPH, currLeafNum)))
            //  return (rc);
            if((rc = BeginScan(currLeafPH, currLeafNum)))
                return (rc);
            currKey = nextNextKey; // store the current key.
            scanStarted = true; // scan has now started. set the indicator
            SetRID(true);       // Set the current RID
            // Get the next value. If none exist, mark the end of the scan has been reached.
            if((IX_EOF == FindNextValue()))
                endOfIndexReached = true;
        }
        else{
            currKey = nextKey; // Otherwise, continue the scan by updating the current value
            currRID = nextRID; // to the one in the buffer (nextRID/nextKey)
        }
        SetRID(false); // Set the element in the buffer to the current state of the scan
        nextKey = nextNextKey;

        if((IX_EOF == FindNextValue())){ // advance the scan by one step
            endOfIndexReached = true;
        }

        PageNum thisRIDPage;
        if((rc = currRID.GetPageNum(thisRIDPage))) // check that the current RID is not
            return (rc);                             // invalid values. If so, then the
        if(thisRIDPage == -1){                     // end of the scan has been reached, so return IX_EOF
            scanEnded = true;
            return (IX_EOF);
        }

        // If found the next satisfying value, then set the RID to return, and break
        if(compOp == NO_OP){
            rid = currRID;
            notFound = false;
            foundFirstValue = true;
        }
        else if((comparator((void *)currKey, value, attrType, attrLength))){
            rid = currRID;
            notFound = false;
            foundFirstValue = true;
        }
        else if(foundFirstValue == true){
            foundLastValue = true;
            return (IX_EOF);
        }

    }
    SlotNum thisRIDpage;
    currRID.GetSlotNum(thisRIDpage);
    return (rc);
}

/*
 * This sets one of the private variable RIDs. If setCurrent is true, it sets
 * currRID. If setCurrent is false, it sets nextRID.
 */
RC IX_IndexScan::SetRID(bool setCurrent){
    if(endOfIndexReached && setCurrent == false){
        RID rid1(-1,-1); // If we have reached the end of the scan, set the nextRID
        nextRID = rid1;  // to an invalid value.
        return (0);
    }

    if(setCurrent){
        if(hasLeafPinned){ // otherwise, use the leaf value to set page/slot of RID
            RID rid1(leafEntries[leafSlot].page, leafEntries[leafSlot].slot);
            currRID = rid1;
        }
    }
    else{
        if(hasLeafPinned){
            RID rid1(leafEntries[leafSlot].page, leafEntries[leafSlot].slot);
            nextRID = rid1;
        }
    }

    return (0);
}

/*
 * This function adavances the state of the search by one element. It updates all the
 * private variables assocaited with this scan.
 */
RC IX_IndexScan::FindNextValue(){
    RC rc = 0;

    // otherwise, deal with leaf level.
    int prevLeafSlot = leafSlot;
    leafSlot = leafEntries[prevLeafSlot].nextSlot; // update to the next leaf slot

    // If the leaf slot containts duplicate entries, open up the bucket associated
    // with it, and update the key.
    if(leafSlot != NO_MORE_SLOTS && leafEntries[leafSlot].isValid == OCCUPIED_DUP){
        nextNextKey = leafKeys + leafSlot * attrLength;
        currBucketNum = leafEntries[leafSlot].page;
        return (0);
    }
    // Otherwise, stay update the key.
    if(leafSlot != NO_MORE_SLOTS && leafEntries[leafSlot].isValid == OCCUPIED_NEW){
        nextNextKey = leafKeys + leafSlot * attrLength;
        return (0);
    }

    // otherwise, get the next page
    PageNum nextLeafPage = leafHeader->nextPage;

    // if it's not the root page, unpin it:
    if((currLeafNum != (indexHandle->header).rootPage)){
        if((rc = (indexHandle->pfh).UnpinPage(currLeafNum))){
            return (rc);
        }
    }
    hasLeafPinned = false;

    // If the next page is a valid page, then retrieve the first entry in this leaf page
    if(nextLeafPage != NO_MORE_PAGES){
        currLeafNum = nextLeafPage;
        if((rc = (indexHandle->pfh).GetThisPage(currLeafNum, currLeafPH)))
            return (rc);
        if((rc = GetFirstEntryInLeaf(currLeafPH) ))
            return (rc);
        return (0);
    }

    return (IX_EOF); // Otherwise, no more elements

}



RC IX_IndexScan::GetAppropriateEntryInLeaf(PF_PageHandle &leafPH){
    RC rc = 0;
    hasLeafPinned = true;
    if((rc = leafPH.GetData((char *&) leafHeader)))
        return (rc);

    if(leafHeader->num_keys == 0)
        return (IX_EOF);

    leafEntries = (struct Node_Entry *)((char *)leafHeader + (indexHandle->header).entryOffset_N);
    leafKeys = (char *)leafHeader + (indexHandle->header).keysOffset_N;
    int index = 0;
    bool isDup = false;
    if((rc = indexHandle->FindNodeInsertIndex((struct IX_NodeHeader *)leafHeader, value, index)))
        return (rc);

    leafSlot = index;
    if((leafSlot != NO_MORE_SLOTS))
        nextNextKey = leafKeys + attrLength* leafSlot;
    else
        return (IX_INVALIDSCAN);

    if(leafEntries[leafSlot].isValid == OCCUPIED_DUP){ // if it's a duplciate value, go into the bucket
        currBucketNum = leafEntries[leafSlot].page;      // to retrieve the first entry
    }
    return (0);
}


/*
 * This function cleans up the scan. It unpins all the pages that the scan currently has pinned,
 * and frees any values that it malloced.
 */
RC IX_IndexScan::CloseScan(){
    RC rc = 0;
    if(openScan == false)
        return (IX_INVALIDSCAN);
    if(scanEnded == false && hasBucketPinned == true)
        indexHandle->pfh.UnpinPage(currBucketNum);
    if(scanEnded == false && hasLeafPinned == true && (currLeafNum != (indexHandle->header).rootPage))
        indexHandle->pfh.UnpinPage(currLeafNum);
    if(initializedValue == true){
        free(value);
        initializedValue = false;
    }
    openScan = false;
    scanStarted = false;

    return (rc);
}