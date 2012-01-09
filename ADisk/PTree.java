/*
 * PTree -- persistent tree
 *
 * You must follow the coding standards distributed
 * on the class web page.
 *
 * (C) 2007 Mike Dahlin
 *
 */


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class PTree{
  public static final int METADATA_SIZE = 64;
  public static final int MAX_TREES = 512;
   

  //
  // Arguments to getParam
  //
  public static final int ASK_FREE_SPACE = 997;
  public static final int ASK_MAX_TREES = 13425;
  public static final int ASK_FREE_TREES = 23421;

  //
  // TNode structure
  //
  //Number of direct block stored in a tnode
  public static final int TNODE_DIRECT = 8;
  //Number of indirect blocks stored in a tnode
  public static final int TNODE_INDIRECT = 1;
  //Number of double indirect blocks stored in a tnode
  public static final int TNODE_DOUBLE_INDIRECT = 1;
  //Size of a block - always a multiple of sector size
  public static final int BLOCK_SIZE_BYTES = 1024;
  //Number of blocks/internal node or internal nodes/double internal node
  public static final int POINTERS_PER_INTERNAL_NODE = 256;
  //The first thing after the log is the sector allocation map
  public static final int SECTOR_ALLOCATION_MAP_START_LOCATION = ADisk.REDO_LOG_SECTORS + 1;
  //We add it by 31 because the location pointers are inclusive
  public static final int SECTOR_ALLOCATION_MAP_END_LOCATION = SECTOR_ALLOCATION_MAP_START_LOCATION + 31;
  //TNode allocation is a boolean byte array storing the "512" TNodeID usage. It is designed to expand
  public static final int TNODE_ALLOCATION_MAP_START_LOCATION = SECTOR_ALLOCATION_MAP_END_LOCATION + 1;
  //Location address are inclusive and hence we subtract 1 from it
  public static final int TNODE_ALLOCATION_MAP_END_LOCATION = TNODE_ALLOCATION_MAP_START_LOCATION + MAX_TREES / Disk.SECTOR_SIZE - 1; 
  //This implementation can support up to two tnodes per sector or more 
  public static final int NUM_TNODES_PER_SECTOR = 2;
  public static final int TNODE_SIZE = 175;
 
  public static final int TNODE_ARRAY_START = TNODE_ALLOCATION_MAP_END_LOCATION  + 1;
  //255 is used instead of 256 because the indices are inclusive
  public static final int TNODE_ARRAY_END = TNODE_ARRAY_START + 255;
  
  public static final int DATA_SECTOR_START_LOCATION = TNODE_ARRAY_END + 1;
  
  
  public static final int MAX_BLOCK_ID = TNODE_DIRECT + TNODE_INDIRECT*POINTERS_PER_INTERNAL_NODE + TNODE_DOUBLE_INDIRECT * POINTERS_PER_INTERNAL_NODE*POINTERS_PER_INTERNAL_NODE;
  //
  // Globals
  //
 public  ADisk adisk;
  
  public PTree(boolean doFormat) throws IllegalArgumentException, IOException
  {
	  adisk = new ADisk(doFormat);
  }

  public TransID beginTrans()
  {
    return adisk.beginTransaction();
  }

  public void commitTrans(TransID xid) 
    throws IOException, IllegalArgumentException
  {
	  adisk.commitTransaction(xid);
  }

  public void abortTrans(TransID xid) 
    throws IOException, IllegalArgumentException
  {
	  adisk.abortTransaction(xid);
  }

  public int createTree(TransID xid) 
    throws IOException, IllegalArgumentException, ResourceException
  {
	//Create an entry in the inode array
	//Allocate space for a TNode and fill it with 0s.
	  //TNode newTree = new TNode();
	  
	  //Find a TNode which is not yet used
	  int TNodeNum = getFreeTNodeNum(xid);
	  
	  //Find the actual place the TNode is stored
	  int TNodeLocation = getTNodeLocation(TNodeNum,xid);
	  
	  //Within the sector this TNode is stored in a particular location
	  int TNodeInternalLocation = getTNodeInternalLocation(TNodeNum );

	  //Create an empty tree and convert it into a byte buffer to be written to disk
	  byte[] newTreeBuffer = toByteArray(new TNode());

	  //Within this sector, we know the location, we need to modify only those bytes
	  //So first read the entire sector
	  byte[] buffer = new byte[Disk.SECTOR_SIZE];
	  adisk.readSector(xid, TNodeLocation, buffer);
	  
	  //Modify the ith part of the buffer variable where this tree is stored
	  System.arraycopy(newTreeBuffer, 0, buffer, TNodeInternalLocation * PTree.TNODE_SIZE , Math.min(PTree.TNODE_SIZE,newTreeBuffer.length) );

	  //Once modified write it back to the sector.
	  adisk.writeSector(xid, TNodeLocation, buffer);
	  
	  return TNodeNum;
  }

  public boolean deleteTree(TransID xid, int tnum)
    throws IOException, IllegalArgumentException
  {
	  //Get the tnode data structure
	  //Find the actual place the TNode is stored
	  int TNodeLocation = getTNodeLocation(tnum,xid);
	  
	  //Within the sector this TNode is stored in a particular location
	  int TNodeInternalLocation = getTNodeInternalLocation(tnum);

	  //Within this sector, we know the internal location, we need to read only those bytes
	  //So first read the entire sector
	  byte[] sectorData = new byte[Disk.SECTOR_SIZE];
	  adisk.readSector(xid, TNodeLocation, sectorData);
	  
	  //Copy only this tree from the sector
	  byte[] TNodeByteData = new byte[PTree.TNODE_SIZE];
	  System.arraycopy(sectorData,TNodeInternalLocation * PTree.TNODE_SIZE , TNodeByteData, 0 , PTree.TNODE_SIZE);
	  
	  //Convert the tree buffer into a tree instance
	  TNode tnode = toTNode(TNodeByteData);
	  
	  //free all  blocks that the tree has allocated for itself
	  //NOTE: since we are going to delete the tnode, i am not storing in the tnode that the bytes are deallocated
	  //Cycle through each direct block in the tnode, and deallocate it
	  for(int ii = 0 ; ii < PTree.TNODE_DIRECT ; ii++)
	  {
		  if(tnode.directBlockPointers[ii] != 0)
		  {
			  this.deallocateBlock(xid, tnode.directBlockPointers[ii]);
		  }
	  }
	  
	  //Now do all the internal nodes
	  for(int ii = 0 ; ii < PTree.TNODE_INDIRECT; ii++)
	  {
		  if(tnode.indirectNodePointers[ii] != 0)
		  {
			  this.deallocateInternalNode(xid, tnode.indirectNodePointers[ii]);
			  this.deallocateBlock(xid, tnode.indirectNodePointers[ii]);
		  }
	  }
	  
	  //Now do all the double internal nodes
	  for(int ii = 0 ; ii < PTree.TNODE_DOUBLE_INDIRECT; ii++)
	  {
		  //Check whether the doubld inidrect pointer exists, if so, delete it
		  if(tnode.doubleIndirectNodePointers[ii] != 0)
		  {
			  //Before deleting the pointer, delete the internal nodes in the pointer 
			  byte[] blockData = new byte[PTree.BLOCK_SIZE_BYTES];
			  byte[] intByte = new byte[4];
			  this.readBufferFromBlock(xid, tnode.doubleIndirectNodePointers[ii], blockData);
			  for(int jj = 0 ; jj < PTree.POINTERS_PER_INTERNAL_NODE; jj++)
			  {
				  System.arraycopy(blockData, jj*4, intByte, 0, 4);
				  int internalNodeSectorNum = Transaction.bytesToInt(intByte);
				  if(internalNodeSectorNum != 0)
				  {
					  this.deallocateInternalNode(xid, internalNodeSectorNum);
					  this.deallocateBlock(xid, internalNodeSectorNum);
				  }
			  }
			  this.deallocateBlock(xid, tnode.doubleIndirectNodePointers[ii]);
	  	  }
	  }
	  
	  //free block bitmap allocation space - not implemented
	  
	  boolean result = freeByteAtOffset(xid,PTree.TNODE_ALLOCATION_MAP_START_LOCATION,PTree.TNODE_ALLOCATION_MAP_END_LOCATION, 1 , tnum);
	  return result;
  
  }
  
  public int getMaxDataBlockId(TransID xid, int tnum)
    throws IOException, IllegalArgumentException
  {
	  //Create the TNode structure
	  //walk through the tnode in reverse order and return the first block id
	  
	  //Get the tnode data structure
	  //Find the actual place the TNode is stored
	  int TNodeLocation = getTNodeLocation(tnum,xid);
	  
	  //Within the sector this TNode is stored in a particular location
	  int TNodeInternalLocation = getTNodeInternalLocation(tnum);

	  //Within this sector, we know the internal location, we need to read only those bytes
	  //So first read the entire sector
	  byte[] sectorData = new byte[Disk.SECTOR_SIZE];
	  adisk.readSector(xid, TNodeLocation, sectorData);
	  
	  //Copy only this tree from the sector
	  byte[] TNodeByteData = new byte[PTree.TNODE_SIZE];
	  System.arraycopy(sectorData,TNodeInternalLocation * PTree.TNODE_SIZE , TNodeByteData, 0 , PTree.TNODE_SIZE);
	  
	  //Convert the tree buffer into a tree instance
	  TNode tnode = toTNode(TNodeByteData);

	  //Start with the double internal nodes
	  for(int ii = PTree.TNODE_DOUBLE_INDIRECT - 1 ; ii >= 0 ; ii--)
	  {
		  //Check whether the double indirect pointer exists, if so, scan it
		  if(tnode.doubleIndirectNodePointers[ii] != 0)
		  {
			  //Scan the double internal nodes to get the list of internal nodes in the buffer 
			  byte[] blockData = new byte[PTree.BLOCK_SIZE_BYTES];
			  byte[] intByte = new byte[4];
			  this.readBufferFromBlock(xid, tnode.doubleIndirectNodePointers[ii], blockData);
			  for(int jj = PTree.POINTERS_PER_INTERNAL_NODE - 1 ; jj >=0 ; jj--)
			  {
				  System.arraycopy(blockData, jj*4, intByte, 0, 4);
				  int internalNodeSectorNum = Transaction.bytesToInt(intByte);
				  if(internalNodeSectorNum != 0)
				  {
					  int lastBlockIndex = this.getLastNonZeroIndex(xid,internalNodeSectorNum);
					  if(lastBlockIndex != -1 )
					  {
						  int returnVal = 
						  PTree.TNODE_DIRECT +
						  PTree.TNODE_INDIRECT * PTree.POINTERS_PER_INTERNAL_NODE + 
						  PTree.POINTERS_PER_INTERNAL_NODE * PTree.POINTERS_PER_INTERNAL_NODE * ii + 
						  PTree.POINTERS_PER_INTERNAL_NODE * jj + 
						  lastBlockIndex;
						  return returnVal;
					  }
				  }
			  }
	  	  }
	  }
	  
	  //Now do all the internal nodes
	  for(int ii = PTree.TNODE_INDIRECT - 1 ; ii >=0 ; ii--)
	  {
		  if(tnode.indirectNodePointers[ii] != 0)
		  {
			  int lastBlockIndex = this.getLastNonZeroIndex(xid,tnode.indirectNodePointers[ii]);
			  if(lastBlockIndex != -1)
			  {
				  int returnVal = PTree.TNODE_DIRECT + ii * PTree.POINTERS_PER_INTERNAL_NODE + lastBlockIndex;
				  return returnVal;
			  }
		  }
	  }
	  
	  //Now go to the internal nodes

	  //Cycle through each direct block in the tnode, and deallocate it
	  for(int ii = PTree.TNODE_DIRECT - 1 ; ii >=0 ; ii--)
	  {
		  if(tnode.directBlockPointers[ii] != 0)
			  return ii;

	  }
	  return 0;
  }

  public void readData(TransID xid, int tnum, int blockId, byte buffer[])
    throws IOException, IllegalArgumentException
  {
	  //getBlockSectorNum: gets the 1st sectorNum corresponding to the first 
	  //of the two sectors associated with this block of this tree
	  
	  if(blockId > PTree.MAX_BLOCK_ID || blockId < 0)
		  throw new IllegalArgumentException();
	  
	  if(buffer.length < PTree.BLOCK_SIZE_BYTES)
		  throw new IllegalArgumentException();
	  
	  int sectorNum = this.getBlockSectorNum(blockId, tnum, xid,false);

	  //If the sector does not exist, just return a 0 filled buffer
	  if(sectorNum == 0)
	  {
		  ADiskUnit.setBuffer((byte)0, buffer);
		  return;
	  }
	  
	  this.readBufferFromBlock(xid, sectorNum, buffer);
  }

  public void writeData(TransID xid, int tnum, int blockId, byte buffer[])
    throws IOException, IllegalArgumentException
  {
	  //Block ids between the given block id and the max block id must be set to 0.
	  //internal node and double internal node pointers must be setup if they need to be.
	  
	  //getBlockSectorNum: gets the 1st sectorNum corresponding to the first of 
	  //the two sectors associated with this block of this tree
	  if(blockId > PTree.MAX_BLOCK_ID || blockId < 0)
		  throw new IllegalArgumentException();
	  
	  if(buffer.length < PTree.BLOCK_SIZE_BYTES)
		  throw new IllegalArgumentException();

	  //Get the block. If it is not allocated, allocate sectors by setting the last parameter to true
	  int sectorNum = this.getBlockSectorNum(blockId, tnum, xid,true);
	  
	  //Write the stuff to the block
	  writeBufferToBlock(xid, sectorNum, buffer);
  }

  public void readTreeMetadata(TransID xid, int tnum, byte returnMetaDataBuffer[])
    throws IOException, IllegalArgumentException
  {
	  //Find the actual place the TNode is stored
	  int TNodeLocation = getTNodeLocation(tnum,xid);
	  
	  //Within the sector this TNode is stored in a particular location
	  int TNodeInternalLocation = getTNodeInternalLocation(tnum);

	  //Within this sector, we know the internal location, we need to read only those bytes
	  //So first read the entire sector
	  byte[] sectorData = new byte[Disk.SECTOR_SIZE];
	  adisk.readSector(xid, TNodeLocation, sectorData);
	  
	  //Copy only this tree from the sector
	  byte[] TNodeByteData = new byte[PTree.TNODE_SIZE];
	  System.arraycopy(sectorData,TNodeInternalLocation * PTree.TNODE_SIZE , TNodeByteData, 0 , PTree.TNODE_SIZE);
	  
	  //Convert the buffer into the tree
	  TNode tnode = toTNode(TNodeByteData);
	  System.arraycopy(tnode.metadata1, 0,returnMetaDataBuffer , 0, PTree.METADATA_SIZE );
  }

  public void writeTreeMetadata(TransID xid, int tnum, byte treeMetaData[])
    throws IOException, IllegalArgumentException
  {
	  //Find the actual place the TNode is stored
	  int TNodeLocation = getTNodeLocation(tnum,xid);
	  
	  //Within the sector this TNode is stored in a particular location
	  int TNodeInternalLocation = getTNodeInternalLocation(tnum);

	  //Within this sector, we know the internal location, we need to read only those bytes
	  //So first read the entire sector
	  byte[] sectorData = new byte[Disk.SECTOR_SIZE];
	  adisk.readSector(xid, TNodeLocation, sectorData);
	  
	  //Copy only this tree from the sector
	  byte[] TNodeByteData = new byte[PTree.TNODE_SIZE];
	  System.arraycopy(sectorData,TNodeInternalLocation * PTree.TNODE_SIZE , TNodeByteData, 0 , PTree.TNODE_SIZE);
	  
	  //Convert the tree buffer into a tree instance
	  TNode tnode = toTNode(TNodeByteData);
	  System.arraycopy(treeMetaData, 0, tnode.metadata1, 0, PTree.METADATA_SIZE );
	  
	  //convert it into a byte buffer to be written to disk
	  byte[] newTNodeByteData = toByteArray(tnode);
	  
	  //Write TNodeBuffer to the sector
	  //Modify the ith part of the buffer variable where this tree is stored
	  System.arraycopy(newTNodeByteData, 0, sectorData, TNodeInternalLocation * PTree.TNODE_SIZE , Math.min(newTNodeByteData.length, sectorData.length));

	  //Once modified write it back to the sector.
	  adisk.writeSector(xid, TNodeLocation, sectorData);

  }

  public int getParam(int param)
    throws IOException, IllegalArgumentException
  {
	switch(param)
	{
	case PTree.ASK_FREE_SPACE:
		int freeSpace = this.getCountZeros(PTree.SECTOR_ALLOCATION_MAP_START_LOCATION, PTree.SECTOR_ALLOCATION_MAP_END_LOCATION);
		return freeSpace;
	case PTree.ASK_MAX_TREES:
		return PTree.MAX_TREES;
	case PTree.ASK_FREE_TREES:
		int freeTrees = this.getCountZeros(PTree.TNODE_ALLOCATION_MAP_START_LOCATION,PTree.TNODE_ALLOCATION_MAP_END_LOCATION);
		return freeTrees;
	default:
		throw new IllegalArgumentException();
	}
  }

  private int getTNodeLocation(int TNodeNum, TransID xid)
    throws IndexOutOfBoundsException, IOException
  {
	  //check in the TNODE allocation bitmap whether this tree is valid
	  int TNodeAllocationInfoLocation = PTree.TNODE_ALLOCATION_MAP_START_LOCATION + TNodeNum / Disk.SECTOR_SIZE;
	  int offset  = TNodeNum % Disk.SECTOR_SIZE;
	  byte[]  sectorData = new byte[Disk.SECTOR_SIZE]; 
	  adisk.readSector(xid, TNodeAllocationInfoLocation , sectorData);
	  if(sectorData[offset] == (byte)1)
	  {
		  int TNodeLocation = PTree.TNODE_ARRAY_START + TNodeNum / PTree.NUM_TNODES_PER_SECTOR;
		  return TNodeLocation;
	  }
	  else
		  throw new IndexOutOfBoundsException();
  }

  private int getTNodeInternalLocation(int TNodeNum)
  {
	  int TNodeInternalLocation = TNodeNum % PTree.NUM_TNODES_PER_SECTOR;
	  return TNodeInternalLocation;
  }
  
  private int getFreeTNodeNum(TransID xid)
    throws IndexOutOfBoundsException, IOException
  {
	  int TNodeNum = searchForByteZero(xid, PTree.TNODE_ALLOCATION_MAP_START_LOCATION, PTree.TNODE_ALLOCATION_MAP_END_LOCATION , 1) ;

	  //If we dont find an empty TNode throw an exception
	  if(TNodeNum == -1)
		  throw new ResourceException();
	  else
		  return TNodeNum;
  }
  
  //The following two functions were lifted verbatim from 
  //  http://scr4tchp4d.blogspot.com/2008/07/object-to-byte-array-and-byte-array-to.html
  //I hope this is not considered bad practice as this is conceptually non-challenging and
  //is an insignificant part of this project
  private static byte[] toByteArray (TNode tnode)
  {
    byte[] bytes = null;
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try {
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      tnode.writeObject(oos);
      oos.flush(); 
      oos.close();
      bytes = bos.toByteArray ();
      bos.close();
      
    }
    catch (IOException ex) {
    	ex.printStackTrace();
    }
    return bytes;
  }
      
  private static TNode toTNode (byte[] bytes)
  {
    TNode tnode = new TNode();
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream (bytes);
      ObjectInputStream ois = new ObjectInputStream (bis);
      tnode.readObject(ois);
      //tnode = (TNode)ois.readObject();
    }
    catch (IOException ex) {
    	ex.printStackTrace();
    }
    catch (ClassNotFoundException ex) {
    	ex.printStackTrace();
    }
    return tnode;
  }
	
  private int getBlockSectorNum(int blockId, int tnum, TransID xid, boolean allocateIfMT)
    throws IllegalArgumentException, IndexOutOfBoundsException, IOException
  {
	  int range1 = PTree.TNODE_DIRECT ;
	  int range2 = range1 + PTree.TNODE_INDIRECT * PTree.POINTERS_PER_INTERNAL_NODE;
	  int range3 = range2 + PTree.TNODE_DOUBLE_INDIRECT * PTree.POINTERS_PER_INTERNAL_NODE * PTree.POINTERS_PER_INTERNAL_NODE;
	  int sectorNum = 0;
	  //There is a good possibility that a block id does not exist, return 0 in such cases
	  
	  //Find the actual place the TNode is stored
	  int TNodeLocation = getTNodeLocation(tnum, xid);
	  
	  //Within the sector this TNode is stored in a particular location
	  int TNodeInternalLocation = getTNodeInternalLocation(tnum);

	  //Within this sector, we know the internal location, we need to read only those bytes
	  //So first read the entire sector
	  byte[] sectorData = new byte[Disk.SECTOR_SIZE];
	  adisk.readSector(xid, TNodeLocation, sectorData);
	  
	  //Copy only this tree from the sector
	  byte[] TNodeByteData = new byte[PTree.TNODE_SIZE];
	  System.arraycopy(sectorData,TNodeInternalLocation * PTree.TNODE_SIZE , TNodeByteData, 0 , PTree.TNODE_SIZE);
	  
	  //Convert the tree buffer into a tree instance
	  TNode tnode = toTNode(TNodeByteData);
	  boolean tnodeModified = false;
	  
	  if(blockId < 0 || blockId > PTree.MAX_BLOCK_ID)
		  throw new IllegalArgumentException();

	  else if(blockId < range1)
	  {
		  //get the block from directly accessing the link in the TNode object
		  sectorNum = tnode.directBlockPointers[blockId];
		  
		  if(sectorNum == 0 && allocateIfMT)
		  {
			  //Modify the byte array to include the new sector number
			  sectorNum = allocateBlock(xid);
			  //Modify the TNode direct block pointers
			  tnode.directBlockPointers[blockId] = sectorNum;
			  tnodeModified = true;
		  }  
		  if(sectorNum == 0 && !allocateIfMT)
		  {
			  return 0; 
		  }
		  
	  }
	  else if(blockId < range2)
	  {
		  //get the block from the pointer in the appropriate internal node
		  //Program must be designed to support more than 1 internal and double internal nodes
		  
		  //Get the indirect node index. For the current setting it is always 0
		  int indirectNodeIndex = ( blockId - range1 ) / PTree.POINTERS_PER_INTERNAL_NODE;
		  
		  //Within the node pointed by the indirect node, the required sector number is present after an offset. Find that
		  int offset = ( blockId - range1 ) % PTree.POINTERS_PER_INTERNAL_NODE;
		  
		  //Get the indirect node pointer as mentioned in the tnode array
		  int indirectNodeSectorNum = tnode.indirectNodePointers[indirectNodeIndex];

		  //If the internal node is not allocate, allocate it first and the allocate the space for the block inside it
		  if(indirectNodeSectorNum == 0 && allocateIfMT)
		  {
			  //Create a new block for storing this block Id and store the value in the tnode array
			  indirectNodeSectorNum = allocateBlock(xid);
			  tnode.indirectNodePointers[indirectNodeIndex] = indirectNodeSectorNum;
			  tnodeModified = true;
		  }
		  if(indirectNodeSectorNum == 0 && !allocateIfMT)
		  {
			  return 0; 
		  }
		  //Get that sector number using this function
		  sectorNum = getIntegerInSectorNum(indirectNodeSectorNum,offset,xid);
		  if(sectorNum == 0 && allocateIfMT)
		  {
			  //Modify the byte array to include the new sector number
			  sectorNum = allocateBlock(xid);
			  //Set the pointer in the indirect node sector 
			  this.setIntegerInSectorNum(indirectNodeSectorNum,offset,sectorNum,xid);
			  tnodeModified = true;
		  }
		  if(sectorNum == 0 && !allocateIfMT)
		  {
			  return 0; 
		  }
	  }
	  else if(blockId < range3)
	  {
		  //get the block from the pointer in the appropriate double internal node
		  //WARNING: Program must be designed to support more than 1 internal and double internal nodes
		  int tempSize = PTree.POINTERS_PER_INTERNAL_NODE * PTree.POINTERS_PER_INTERNAL_NODE;
		  //Get the double indirect node index. For the current setting it is always 0
		  int doubleIndirectNodeIndex = ( blockId - range2 ) / tempSize;
		  
		  //Get the double indirect node pointer as mentioned in the TNode array
		  int doubleIndirectNodeSectorNum = tnode.doubleIndirectNodePointers[doubleIndirectNodeIndex];
		  
		  if(doubleIndirectNodeSectorNum == 0 && allocateIfMT)
		  {
			  //Create a new block for storing this block Id and store the value in the tnode array
			  doubleIndirectNodeSectorNum = allocateBlock(xid);
			  tnode.doubleIndirectNodePointers[doubleIndirectNodeIndex] = doubleIndirectNodeSectorNum;
			  tnodeModified = true;
		  }
		  if(doubleIndirectNodeSectorNum == 0 && !allocateIfMT)
		  {
			  return 0; 
		  }

		  //Within the node pointed by the double indirect node, the required indirect node pointer is present after an offset. Find that
		  int offset = (( blockId - range2 ) / PTree.POINTERS_PER_INTERNAL_NODE ) % tempSize;
		  
		  //Find the indirect node
		  int indirectNodeSectorNum = getIntegerInSectorNum(doubleIndirectNodeSectorNum,offset,xid);
		  
		  if(indirectNodeSectorNum == 0 && allocateIfMT)
		  {
			  //Create a new block for storing this block Id and store the value in the tnode array
			  indirectNodeSectorNum = allocateBlock(xid);
			  //Set the pointer in the double indirect node sector 
			  this.setIntegerInSectorNum(doubleIndirectNodeSectorNum,offset,indirectNodeSectorNum,xid);
			  tnodeModified = true;
		  }
		  if(indirectNodeSectorNum == 0 && !allocateIfMT)
		  {
			  return 0; 
		  }
		  
		  //Within the node pointed by the indirect node, the required sector number is present after an offset. Find that
		  offset = ( blockId - range2 - offset * PTree.POINTERS_PER_INTERNAL_NODE );
		  
		  //Get that sector number using this function
		  sectorNum = getIntegerInSectorNum(indirectNodeSectorNum,offset,xid);
		  if(sectorNum == 0 && allocateIfMT)
		  {
			  //Modify the byte array to include the new sector number
			  sectorNum = allocateBlock(xid);
			  //Set the pointer in the indirect node sector 
			  this.setIntegerInSectorNum(indirectNodeSectorNum,offset,sectorNum,xid);
			  tnodeModified = true;
		  }
		  if(sectorNum == 0 && !allocateIfMT)
		  {
			  return 0; 
		  }
	  }
	  else
		  throw new IllegalArgumentException();

	  //If the tree has been modified, write it back to the sector
	  if(tnodeModified)
	  {
		  //convert it into a byte buffer to be written to disk
		  byte[] newTNodeByteData = toByteArray(tnode);
		  
		  //Write TNodeBuffer to the sector
		  //Modify the ith part of the buffer variable where this tree is stored
		  System.arraycopy(newTNodeByteData, 0, sectorData, TNodeInternalLocation * PTree.TNODE_SIZE , Math.min(newTNodeByteData.length, sectorData.length));

		  //Once modified write it back to the sector.
		  adisk.writeSector(xid, TNodeLocation, sectorData);

	  }
	  
	  return sectorNum;
  }
  
  private void setIntegerInSectorNum(int sectorNum, int offset, int intVal, TransID xid)
    throws IndexOutOfBoundsException, IOException {
	  assert offset < 256;
	  assert sectorNum != 0;
	  byte[] sectorData = new byte[Disk.SECTOR_SIZE];
	  //Originally it was 512. But that is a mistake as offset indicates 
	  // a 4 byte integer offset. Thus we use 512/4 = 128
	  if (offset >= 128){
		  sectorNum++;
		  offset = offset - 128;
	  }
	  
  
	  adisk.readSector(xid, sectorNum, sectorData );
	  System.arraycopy(Transaction.intToBytes(intVal), 0,sectorData, offset*4,  4);
	  adisk.writeSector(xid, sectorNum, sectorData);
	  	
}

  private int getIntegerInSectorNum(int sectorNum , int offset, TransID xid)
    throws IndexOutOfBoundsException, IOException
  {
	//Total number of integers that can be stored in two sectors
	  assert offset < 256;
	  assert sectorNum != 0;
	  byte[] sectorData = new byte[Disk.SECTOR_SIZE];
	  //Originally it was 512. But that is a mistake as offset indicates 
	  // a 4 byte integer offset. Thus we use 512/4 = 128
	  if (offset >= 128){
		  sectorNum++;
		  offset = offset - 128;
	  }
	  adisk.readSector(xid, sectorNum, sectorData );
	  byte[] returnIntAsBytes = new byte[4];
	  System.arraycopy(sectorData, offset*4, returnIntAsBytes, 0, 4);

	  int returnInt = Transaction.bytesToInt(returnIntAsBytes);
	  
	  return returnInt;
  }
  
  private int allocateBlock(TransID xid)
    throws ResourceException, IndexOutOfBoundsException, IOException
  {
	  //Consult sector mapping data before allocating a sector
	  int blockSize = PTree.BLOCK_SIZE_BYTES / Disk.SECTOR_SIZE;
	  int freeSectorNum = searchForByteZero(xid,PTree.SECTOR_ALLOCATION_MAP_START_LOCATION,PTree.SECTOR_ALLOCATION_MAP_END_LOCATION, blockSize);
	  if(freeSectorNum == -1)
		  throw new ResourceException();
	  else
	  {
		  int sectorNum = freeSectorNum + PTree.DATA_SECTOR_START_LOCATION;
		  
		  //When a block is allocated format it
		  byte[] buffer = new byte[PTree.BLOCK_SIZE_BYTES];
		  ADiskUnit.setBuffer((byte)0, buffer);
		  writeBufferToBlock(xid, sectorNum, buffer);
		  
		  return sectorNum;
	  }
  }
  
  private boolean deallocateBlock(TransID xid, int sectorNum)
    throws IndexOutOfBoundsException, IOException
  {
	  //Consult sector mapping data before allocating a sector
	  //The following implementation will consider blocks of any size of the multiple of a sector size
	  int blockSize = PTree.BLOCK_SIZE_BYTES / Disk.SECTOR_SIZE;
	  int offset = sectorNum - PTree.DATA_SECTOR_START_LOCATION;
	  boolean result = freeByteAtOffset(xid,PTree.SECTOR_ALLOCATION_MAP_START_LOCATION,PTree.SECTOR_ALLOCATION_MAP_END_LOCATION, blockSize, offset);
	  return result;

  }

  private void writeBufferToBlock(TransID xid, int sectorNum, byte[] buffer)
  {
	  //NOTE: the following implementation supports block of any contiguous sector length
	  byte[] sectorBuffer = new byte[Disk.SECTOR_SIZE];
	  
	  for(int lengthScanned = 0 ; lengthScanned < buffer.length ; lengthScanned += Disk.SECTOR_SIZE)
	  {
		  System.arraycopy(buffer, lengthScanned, sectorBuffer, 0, Disk.SECTOR_SIZE);
		  adisk.writeSector(xid, sectorNum, sectorBuffer);
		  sectorNum++;
	  }
  }
  
  private void readBufferFromBlock(TransID xid, int sectorNum, byte[] buffer)
    throws IndexOutOfBoundsException, IOException
  {
	  byte[] sectorBuffer = new byte[Disk.SECTOR_SIZE];
	  
	  //The following implementation takes into account, cases where block is several multiples of the sector size
	  for(int lengthScanned = 0 ; lengthScanned < PTree.BLOCK_SIZE_BYTES ;lengthScanned+=Disk.SECTOR_SIZE)
	  {
		  adisk.readSector(xid, sectorNum, sectorBuffer);
		  System.arraycopy(sectorBuffer, 0, buffer, lengthScanned,  Disk.SECTOR_SIZE);
		  sectorNum++;
	  }
  }
  
  private boolean freeByteAtOffset(TransID xid, int startLocation, int endLocation, int length,int offset)
    throws IndexOutOfBoundsException, IOException {
	  //Consider all the sectors in the given range
	  int location = offset / Disk.SECTOR_SIZE + startLocation;
	  int internalLocation = offset % Disk.SECTOR_SIZE;
	  byte[] sectorData = new byte[Disk.SECTOR_SIZE];
	  //Read the sector where the offset is present
	  adisk.readSector(xid, location, sectorData);
	  for(int jj = 0 ; jj < length ;jj++ )
	  {
		  if(sectorData[internalLocation + jj] != (byte)1)
			  throw new IndexOutOfBoundsException();
	  }
	  //If it is all right, set the corresponding bytes to 0
	  for(int jj = 0 ; jj < length ;jj++ )
		  sectorData[internalLocation + jj] = (byte)0;
	  
	  //Make a note in the TNode allocation mapping that the particular TNode has been used
	  adisk.writeSector(xid, location, sectorData);
	  	  
	  return true;
  }
  
  private int searchForByteZero(TransID xid, int startLocation, int endLocation, int length)
    throws IndexOutOfBoundsException, IOException
  {
	  byte[] buffer = new byte[Disk.SECTOR_SIZE];
	  boolean found = false;
	  int matchByteLocation= -1;
	  
	  //Consider all the sectors in the given range
	  for(int sectorNum = startLocation ; !found && sectorNum <= endLocation; sectorNum++ )
	  {
		  //Read a sector
		  adisk.readSector(xid, sectorNum, buffer);
		  for(int ii = 0 ; ii < buffer.length && !found ; ii++ , matchByteLocation++)
		  {
			  //Check whether we have 0 for the required length
			  found = true;
			  for(int jj = 0 ; jj < length && found;jj++ )
			  {
				  if(buffer[ii + jj] != (byte)0)
					  found = false;
			  }
			  
			  //If it is found, set the corresponding bytes to 1
			  if(found)
			  {
				  for(int jj = 0 ; jj < length && found;jj++ )
					  buffer[ii + jj] = (byte)1;
			  }
			  
			  //Make a note in the TNode allocation mapping that the particular TNode has been used
			  adisk.writeSector(xid, sectorNum, buffer);
		  }
	  }
	  if(!found)
		  return -1;
	  return matchByteLocation;
  }
  
  private int getCountZeros(int startLocation, int endLocation)
    throws IndexOutOfBoundsException, IOException {
	  byte[] buffer = new byte[Disk.SECTOR_SIZE];
	  int countZeros = 0;
	  TransID xid = this.beginTrans();
	  //Consider all the sectors in the given range
	  for(int sectorNum = startLocation ; sectorNum <= endLocation; sectorNum++ )
	  {
		  //Read a sector
		  adisk.readSector(xid, sectorNum, buffer);
		  for(int ii = 0 ; ii < buffer.length; ii++)
		  {
			  //Check whether we have 0s
			  if(buffer[ii] == (byte)0)
				  countZeros++;
		  }
	  }
	  this.commitTrans(xid);
	  return countZeros;
	}

  private int getLastNonZeroIndex(TransID xid, int indirectNodepointer)
  throws IndexOutOfBoundsException, IOException {
	  byte[] internalNodeData = new byte[PTree.BLOCK_SIZE_BYTES];
	  byte[] intByte = new byte[4];
	  this.readBufferFromBlock(xid, indirectNodepointer, internalNodeData);
	  for(int kk = PTree.POINTERS_PER_INTERNAL_NODE - 1 ; kk >= 0 ; kk--)
	  {
		  System.arraycopy(internalNodeData, kk*4, intByte, 0, 4);
		  int sectorNum = Transaction.bytesToInt(intByte);
		  if(sectorNum != 0)
			  return kk;
	  }
	  return -1;
}
  
private void deallocateInternalNode(TransID xid, int internalNodeSectorNum)
    throws IndexOutOfBoundsException, IOException
  {
	  byte[] blockData = new byte[PTree.BLOCK_SIZE_BYTES];
	  byte[] intByte = new byte[4];
	  this.readBufferFromBlock(xid, internalNodeSectorNum, blockData);
	  for(int jj = 0; jj < PTree.POINTERS_PER_INTERNAL_NODE ;jj++)
	  {
		  System.arraycopy(blockData, jj*4, intByte, 0, 4);
		  int sectorNum = Transaction.bytesToInt(intByte);
		  if(sectorNum != 0)
		  {
			  this.deallocateBlock(xid, sectorNum);
		  }
	  }
  }
}
