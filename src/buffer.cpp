/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University
 * of Wisconsin-Madison.
 */

#include "buffer.h"

#include <iostream>
#include <memory>

#include "exceptions/bad_buffer_exception.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/hash_not_found_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"

namespace badgerdb {

constexpr int HASHTABLE_SZ(int bufs) { return ((int)(bufs * 1.2) & -2) + 1; }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
    : numBufs(bufs),
      hashTable(HASHTABLE_SZ(bufs)),
      bufDescTable(bufs),
      bufPool(bufs) {
  for (FrameId i = 0; i < bufs; i++) {
    bufDescTable[i].frameNo = i;
    bufDescTable[i].valid = false;
  }

  clockHand = bufs - 1;
}


void BufMgr::advanceClock() {
  clockHand = (clockHand + 1)%numBufs;
}

/**
 * @brief Uses the clock algorithm to allocate a free frame
 * @param frame frame reference number 
 * @throws BufferExceededExcpetion if all buffer frames are pinned.
 */ 
void BufMgr::allocBuf(FrameId& frame) {
  
  unsigned int count = 0; // keeps track of the total pages pinned

  // use the clock algorithm
  while(count <= numBufs){ // while the total number of frames have not exceeded
    advanceClock();
    if (!bufDescTable[clockHand].valid){ // allocate a frame if not valid
      frame = bufDescTable[clockHand].frameNo;
      return;
    }
    else if(bufDescTable[clockHand].valid){ // if valid
      if(bufDescTable[clockHand].refbit){
        bufDescTable[clockHand].refbit = false; 
        advanceClock();
      }
      else if(!bufDescTable[clockHand].refbit){
        if (bufDescTable[clockHand].pinCnt != 0){
          count++;
          advanceClock();
        }
        // make sure frame has not been pinned
          // if the frame has not been edited
        else if (bufDescTable[clockHand].pinCnt == 0 && !bufDescTable[clockHand].dirty){ 
          hashTable.remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
          // allocate the frame
          frame = bufDescTable[clockHand].frameNo;
          return;
        }
        else if(bufDescTable[clockHand].pinCnt == 0 && bufDescTable[clockHand].dirty){
          // or else write the page back to the disk 
          flushFile(bufDescTable[clockHand].file);
          frame = bufDescTable[clockHand].frameNo;
          return;
        }
      }
    }
  }
  throw BufferExceededException(); // throw exception if all the pages are pinned
}

}

void BufMgr::readPage(File& file, const PageId pageNo, Page*& page) {}

void BufMgr::unPinPage(File& file, const PageId pageNo, const bool dirty) {}

void BufMgr::allocPage(File& file, PageId& pageNo, Page*& page) {}

void BufMgr::flushFile(File& file) {}

void BufMgr::disposePage(File& file, const PageId PageNo) { 
    FrameId toDispose;
    try{
        hashTable->lookup(file, PageNo, toDispose);
        bufDescTable[toDispose].Clear();
        hashTable->remove(file, PageNo);
    }
    catch(HashNotFoundException e){
    }
    //delete page from the file
    file->deletePage(PageNo);
}

void BufMgr::printSelf(void) {
  int validFrames = 0;

  for (FrameId i = 0; i < numBufs; i++) {
    std::cout << "FrameNo:" << i << " ";
    bufDescTable[i].Print();

    if (bufDescTable[i].valid) validFrames++;
  }

  std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}  // namespace badgerdb
