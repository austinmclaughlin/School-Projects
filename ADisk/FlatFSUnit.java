import java.io.IOException;
import java.util.LinkedList;


public class FlatFSUnit{
	
	public static void main(String args[])
	{
		
		
		try {
			System.out.println("FlatFS testing.");
			FlatFS flatfs = new FlatFS(true);
			int inumber = metadataReadWriteCheck(flatfs);
			directReadWriteCheck(flatfs,inumber);
			randomReadWriteCheck(flatfs,inumber);
			int[] fileList = multipleFileCheck(flatfs);
			deleteFilecheck( flatfs, fileList );
		
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void deleteFilecheck(FlatFS flatfs, int[] inumber) throws IllegalArgumentException, IOException
	{
		TransID xid = flatfs.beginTrans();
		boolean result = true;	
		for(int ii = 0 ; ii < inumber.length;ii++)
		{
			if(!flatfs.deleteFile(xid, inumber[ii]))
			{
				System.out.println("Delete File check - failed. inumber: " + inumber[ii]);
				result = false;
			}
		}
		
		if(result)
			System.out.println("Delete File check - OK.");
		else
			System.out.println("Delete File check - failed");
		
		flatfs.commitTrans(xid);
		return;
	}
	
	public static int[] multipleFileCheck(FlatFS flatfs) throws IllegalArgumentException, IOException
	{
		TransID xid = null;
		
		
		xid = flatfs.beginTrans();
		int inumber1 = flatfs.createFile(xid);
		flatfs.commitTrans(xid);
		
		byte[] metaData = new byte[PTree.METADATA_SIZE];
		ADiskUnit.setBuffer((byte)5, metaData);
		xid = flatfs.beginTrans();
		flatfs.writeFileMetadata(xid, inumber1, metaData);
		flatfs.commitTrans(xid);
		
		
		
		xid = flatfs.beginTrans();
		int inumber2 = flatfs.createFile(xid);
		flatfs.commitTrans(xid);
		
		metaData = new byte[PTree.METADATA_SIZE];
		ADiskUnit.setBuffer((byte)3, metaData);
		xid = flatfs.beginTrans();
		flatfs.writeFileMetadata(xid, inumber2, metaData);
		flatfs.commitTrans(xid);
		
	
		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)0, metaData);
		flatfs.readFileMetadata(xid, inumber1, metaData);
		flatfs.commitTrans(xid);

		if(!ADiskUnit.checkBuffer((byte)5, metaData))
		{
			System.out.println("Multiple tree metadata read/write check - FAILED");
			return null;
		}

		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)0, metaData);
		flatfs.readFileMetadata(xid, inumber2, metaData);
		flatfs.commitTrans(xid);
		
		if(!ADiskUnit.checkBuffer((byte)3, metaData))
		{
			System.out.println("Multiple tree metadata read/write check - FAILED");
			return null;
		}

		System.out.println("Multiple tree metadata read/write check - OK");
		
		return new int[]{inumber1, inumber2};
		
	}
	
	//NOTE: for this read to work, ptree.allocateSector must be implemented 
	//and incorporated into write and read functions appropriately
	public static void randomReadWriteCheck(FlatFS flatfs, int inumber) throws IllegalArgumentException, IOException
	{
				
		int bufferSize = 2500;
		byte[] buffer = new byte[bufferSize ];
		int numWrites = 10;
		int[] locations = new int[numWrites];
		for(int ii = 0 ; ii < numWrites; ii++)
		{
			TransID xid = flatfs.beginTrans();
			int offset = (int)Math.round(Math.random() * 100000);
			locations[ii]=offset;
			ADiskUnit.setBuffer((byte)ii, buffer);
			flatfs.write(xid, inumber, offset, bufferSize , buffer);
			flatfs.commitTrans(xid);
		}
		
		for(int ii = 0 ; ii < numWrites; ii++)
		{
			TransID xid = flatfs.beginTrans();
			int offset = locations[ii];
			ADiskUnit.setBuffer((byte)0, buffer);
			flatfs.read(xid, inumber, offset, bufferSize , buffer);
			if(!ADiskUnit.checkBuffer((byte)ii, buffer , 0 ,2000))
			{
				System.out.println("Random write to indirect nodes failed at location " + offset);
				return;
			}
			else
				System.out.println("Good");
			flatfs.commitTrans(xid);
			
		}

		System.out.println("Random Read/Write check of file - OK");
	}
	
	
	//NOTE: for this read to work, ptree.allocateSector must be implemented 
	//and incorporated into write and read functions appropriately
	public static void directReadWriteCheck(FlatFS flatfs, int inumber) throws IllegalArgumentException, IOException
	{
		TransID xid = null;
		
		byte[] buffer = new byte[PTree.BLOCK_SIZE_BYTES * 1000];
		
		//This lies within the direct block region
		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)1, buffer);
		flatfs.write(xid, inumber, 0, 2500, buffer);
		flatfs.commitTrans(xid);
		
		//This lies at the border of the direct and indirect nodes region
		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)2, buffer);
		flatfs.write(xid, inumber, 7000, 2000, buffer);
		flatfs.commitTrans(xid);
		
		//This lies at the border of the indirect and the double indirect nodes region
		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)3, buffer);
		flatfs.write(xid, inumber, 270335, 2000, buffer);
		flatfs.commitTrans(xid);
		
		//Check the bytes inthe direct block region
		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)0, buffer);
		flatfs.read(xid, inumber, 0, 2500, buffer);
		if(!ADiskUnit.checkBuffer((byte)1, buffer , 0 ,2500))
		{
			System.out.println("Write to direct blocks failed");
			return;
		}
		flatfs.commitTrans(xid);
		

		//Check the bytes at the border of direct and indirect nodes
		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)0, buffer);
		flatfs.read(xid, inumber, 7000, 2000, buffer);
		if(!ADiskUnit.checkBuffer((byte)2, buffer , 0 ,2000))
		{
			System.out.println("Write to indirect nodes failed");
			return;
		}
		flatfs.commitTrans(xid);
		
		//Check the bytes at the border of indirect and double indirect nodes
		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)0, buffer);
		flatfs.read(xid, inumber, 270335, 2000, buffer);
		if(!ADiskUnit.checkBuffer((byte)3, buffer , 0 ,2000))
		{
			System.out.println("Write to indirect nodes failed");
			return;
		}
		flatfs.commitTrans(xid);
		
		System.out.println("Direct Read/Write check of file - OK");
	}

	public static int metadataReadWriteCheck(FlatFS flatfs) throws IllegalArgumentException, IOException
	{
		TransID xid = null;
		
		//1. Create a file
		xid = flatfs.beginTrans();
		int inumber = flatfs.createFile(xid);
		flatfs.commitTrans(xid);
	
		//2. Write metadata to the file
		byte[] metaData = new byte[PTree.METADATA_SIZE];
		ADiskUnit.setBuffer((byte)5, metaData);
		xid = flatfs.beginTrans();
		flatfs.writeFileMetadata(xid, inumber, metaData);
		flatfs.commitTrans(xid);
		
		//3. read metadata from the file
		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)0, metaData);
		flatfs.readFileMetadata(xid, inumber, metaData);	
		flatfs.commitTrans(xid);
		
		//4. Check if they match
		if(!ADiskUnit.checkBuffer((byte)5, metaData))
		{
			System.out.println("MetaData read/write check (Step 1)- FAILED");
			return -1;
		}
		
		//5. Delete the disk and start once again
		flatfs = new FlatFS(false);
		
		//6. See if the file and its metadata still exist
		xid = flatfs.beginTrans();
		ADiskUnit.setBuffer((byte)0, metaData);
		flatfs.readFileMetadata(xid, inumber, metaData);
		flatfs.commitTrans(xid);
		
		if(!ADiskUnit.checkBuffer((byte)5, metaData))
		{
			System.out.println("MetaData read check (Step 2) - FAILED");
			return -1;
		}

		//Everything is working fine 
		System.out.println("MetaData read/write check - OK");

		return inumber;
	}

}
