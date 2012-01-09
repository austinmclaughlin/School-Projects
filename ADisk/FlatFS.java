/*
 * FlatFS -- flat file system
 *
 * You must follow the coding standards distributed
 * on the class web page.
 *
 * (C) 2007 Mike Dahlin
 *
 */

import java.io.IOException;
import java.io.EOFException;
public class FlatFS{

  public static final int ASK_MAX_FILE = 2423;
  public static final int ASK_FREE_SPACE_BLOCKS = 29542;
  public static final int ASK_FREE_FILES = 29545;
  public static final int ASK_FILE_METADATA_SIZE = 3502;

  private PTree ptree;
  
  public FlatFS(boolean doFormat)
    throws IOException
  {
	ptree = new PTree(doFormat);  
  }

  public TransID beginTrans()
  {
    return this.ptree.beginTrans();
  }

  public void commitTrans(TransID xid)
    throws IOException, IllegalArgumentException
  {
	  this.ptree.commitTrans(xid);
  }

  public void abortTrans(TransID xid)
    throws IOException, IllegalArgumentException
  {
	  this.ptree.abortTrans(xid);
  }

  public int createFile(TransID xid)
    throws IOException, IllegalArgumentException
  {
    return this.ptree.createTree(xid);
  }

  public boolean deleteFile(TransID xid, int inumber)
    throws IOException, IllegalArgumentException
  {
	  return this.ptree.deleteTree(xid, inumber);
  }

  public int read(TransID xid, int inumber, int offset, int count, byte buffer[])
    throws IOException, IllegalArgumentException, EOFException
  {
	  int lastByte =PTree.MAX_BLOCK_ID * PTree.BLOCK_SIZE_BYTES;
		if(offset > lastByte)
			  throw new EOFException();
		
		if( offset + count > lastByte)
		{
			count = lastByte - offset;
		}
		
		int startLocation = (int) (Math.floor((double)offset/(double)PTree.BLOCK_SIZE_BYTES));
		int endLocation = (int) Math.ceil(((double)(offset+count))/(double)PTree.BLOCK_SIZE_BYTES);
		
		int offsetWithinBlock = offset % PTree.BLOCK_SIZE_BYTES;
			  
		int bytesStoredInBuffer = 0;
		byte[] blockData = new byte[PTree.BLOCK_SIZE_BYTES];
		for(int ii = startLocation ; ii < endLocation ; ii++)
		{
			this.ptree.readData(xid, inumber, ii, blockData);
			
			for(int jj = offsetWithinBlock ; jj < PTree.BLOCK_SIZE_BYTES && bytesStoredInBuffer <count; jj++ , bytesStoredInBuffer++ )
			{
				buffer[bytesStoredInBuffer] = blockData[jj];
			}
			offsetWithinBlock = 0;
		}
		return bytesStoredInBuffer;
  }
    
  public void write(TransID xid, int inumber, int offset, int count, byte buffer[])
    throws IOException, IllegalArgumentException
  {
		int lastByte =PTree.MAX_BLOCK_ID * PTree.BLOCK_SIZE_BYTES;
			
		if(offset > lastByte)
			  throw new EOFException();
		
		if( offset + count > lastByte)
		{
			count = lastByte - offset;
		}
		
		int startLocation = (int) (Math.floor((double)offset/(double)PTree.BLOCK_SIZE_BYTES));
		
		int endLocation = (int) Math.ceil(((double)(offset+count))/(double)PTree.BLOCK_SIZE_BYTES);
		
		int offsetWithinFirstBlock = offset % PTree.BLOCK_SIZE_BYTES;
		
		int bytesStoredInBuffer = 0;
		byte[] blockData = new byte[PTree.BLOCK_SIZE_BYTES];
		
		//Read first block separately
	
		for(int ii = startLocation ; ii < endLocation ; ii++)
		{
			this.ptree.readData(xid, inumber, ii, blockData);
			
			for(int jj = offsetWithinFirstBlock ; jj < PTree.BLOCK_SIZE_BYTES && bytesStoredInBuffer <count; jj++ , bytesStoredInBuffer++ )
			{
				blockData[jj] = buffer[bytesStoredInBuffer];
			}
			offsetWithinFirstBlock = 0;
			this.ptree.writeData(xid, inumber, ii, blockData);
		}
  }

  public void readFileMetadata(TransID xid, int inumber, byte buffer[])
    throws IOException, IllegalArgumentException
  {
	  this.ptree.readTreeMetadata(xid, inumber, buffer);
  }

  public void writeFileMetadata(TransID xid, int inumber, byte buffer[])
    throws IOException, IllegalArgumentException
  {
	  this.ptree.writeTreeMetadata(xid, inumber, buffer);
  }

  public int getParam(int param)
    throws IOException, IllegalArgumentException
  {
    return this.ptree.getParam(param);
  }
    
}
