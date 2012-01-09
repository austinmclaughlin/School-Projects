import java.io.IOException;
import java.util.LinkedList;


public class PTreeUnit {
	
	public static void main(String args[])
	{
		
		
		try {
			System.out.println("PTree testing.");
			PTree newTS = new PTree(true);
			int tnum = metadata1ReadWriteCheck(newTS);
			newTS = new PTree(false);
			boolean result = metadata1ReadCheck( newTS , tnum);
			if(result)
				System.out.println("Tree is properly written to the disk");
			else
				System.out.println("There is a problem with reading/writing the tree to the disk");
			
			result = directBlockIdReadWriteCheck(newTS,tnum,0,7);
			result = result && directBlockIdReadWriteCheck(newTS,tnum,8,13);
			result = result && directBlockIdReadWriteCheck(newTS,tnum,257,263);
			result = result && directBlockIdReadWriteCheck(newTS,tnum,1000,1013);
			
			if(result)
				System.out.println("Basic Reading and writing to tree is working");
			else
				System.out.println("Problem with basic reading and writing data");
			
			int[] tnumList = multipleTreeCheck(newTS);
			
			int[] locations1 = fillTreeArbitrarily(tnumList[0], newTS, 2 , 10000);
			int[] locations2 = fillTreeArbitrarily(tnumList[0], newTS, 2 , 250);
			int[] locations3 = fillTreeArbitrarily(tnumList[0], newTS, 2 , 10);
			
			//FIXME: the return value as of now is dummy. It should be list of blocks that were deleted
			int[] locations = deleteTNodesCheck( newTS, tnumList[0] );
			
			getMaxBlockIdCheck(newTS); 
			
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void getMaxBlockIdCheck(PTree ptree) throws IllegalArgumentException, IOException
	{
		boolean error = false;
		TransID xid = ptree.beginTrans();
		int tnum = ptree.createTree(xid);
		ptree.commitTrans(xid);
		

		int[] locations = PTreeUnit.fillTreeArbitrarily(tnum, ptree, 10, 10000);
		
		int max = locations[0];
		for(int ii = 0 ; ii < locations.length ; ii++)
			if(max<locations[ii])
				max = locations[ii];
		
		
		xid =  ptree.beginTrans();
		int maxdatablockid = ptree.getMaxDataBlockId(xid, tnum);
		ptree.commitTrans(xid);
		
		if(max != maxdatablockid)
			error = true;
	
		//ptree.deleteTree(xid, tnum);
		if(!error)
			System.out.println("GetMaxBlockId check - OK");
		else
			System.out.println("GetMaxBlockId check - failed");

	}
	
	public static int[] fillTreeArbitrarily(int tnum, PTree ptree, int numWrites, int multiplier) throws IllegalArgumentException, IOException
	{
		
		byte[] buffer = new byte[PTree.BLOCK_SIZE_BYTES];
		int[]locations = new int[numWrites];
		for(int ii = 0 ; ii < numWrites; ii++)
		{
			TransID xid = ptree.beginTrans();
			int blockId = (int)Math.round(Math.random() * multiplier);
			locations[ii]=blockId;
			ADiskUnit.setBuffer((byte)ii, buffer);
			ptree.writeData(xid, tnum, blockId, buffer);
			ptree.commitTrans(xid);
		}
		return locations;
	}
	public static int[] deleteTNodesCheck(PTree ptree, int tnum) throws IllegalArgumentException, IOException
	{
		TransID xid = ptree.beginTrans();
		//TODO: modify stuff such that locations contains the data blocks that were actually deallocated
		int[] locations = new int[10];

		boolean result = ptree.deleteTree(xid, tnum);
		if(result)
			System.out.println("Delete TNode check - OK.");
		else
			System.out.println("Delete TNode check - failed");
		
		ptree.commitTrans(xid);
		return locations;
	}
	
	public static int[] multipleTreeCheck(PTree ptree) throws IllegalArgumentException, ResourceException, IOException
	{
		TransID xid = null;
		
		
		xid = ptree.beginTrans();
		int tnum = ptree.createTree(xid);
		ptree.commitTrans(xid);
		
		byte[] treeMetaData = new byte[PTree.METADATA_SIZE];
		ADiskUnit.setBuffer((byte)5, treeMetaData);
		xid = ptree.beginTrans();
		ptree.writeTreeMetadata(xid, tnum, treeMetaData);
		ptree.commitTrans(xid);
		
		
		xid = ptree.beginTrans();
		ADiskUnit.setBuffer((byte)0, treeMetaData);
		ptree.readTreeMetadata(xid, tnum, treeMetaData);
		ptree.commitTrans(xid);

		if(ADiskUnit.checkBuffer((byte)5, treeMetaData))
			System.out.println("Multiple tree metadata read/write check - OK");
		else
			System.out.println("Multiple tree metadata read/write check - FAILED");
		
		xid = ptree.beginTrans();
		int tnum2 = ptree.createTree(xid);
		ptree.commitTrans(xid);
		
		treeMetaData = new byte[PTree.METADATA_SIZE];
		ADiskUnit.setBuffer((byte)3, treeMetaData);
		xid = ptree.beginTrans();
		ptree.writeTreeMetadata(xid, tnum2, treeMetaData);
		ptree.commitTrans(xid);
		
		
		xid = ptree.beginTrans();
		ADiskUnit.setBuffer((byte)0, treeMetaData);
		ptree.readTreeMetadata(xid, tnum2, treeMetaData);
		ptree.commitTrans(xid);
				
		if(ADiskUnit.checkBuffer((byte)3, treeMetaData))
			System.out.println("Multiple tree metadata read/write check - OK");
		else
			System.out.println("Multiple tree metadata read/write check - FAILED");

		
		xid = ptree.beginTrans();
		ADiskUnit.setBuffer((byte)0, treeMetaData);
		ptree.readTreeMetadata(xid, tnum, treeMetaData);
		ptree.commitTrans(xid);
		
		
		if(ADiskUnit.checkBuffer((byte)5, treeMetaData))
			System.out.println("Multiple tree metadata read/write check - OK");
		else
			System.out.println("Multiple tree metadata read/write check - FAILED");
		
		return new int[]{tnum, tnum2};
		
	}
	
	//NOTE: for this read to work, ptree.allocateSector must be implemented 
	//and incorporated into write and read functions appropriately
	public static boolean directBlockIdReadWriteCheck(PTree ptree, int tnum,int start, int end) throws IllegalArgumentException, IOException
	{
		TransID xid = null;
		
		xid = ptree.beginTrans();
		
		byte[] buffer = new byte[PTree.BLOCK_SIZE_BYTES];
		
		for (int ii = start ; ii < end; ii++)
		{
			ADiskUnit.setBuffer((byte)ii, buffer);
			ptree.writeData(xid, tnum, ii, buffer);
		}
		ptree.commitTrans(xid);
		
		xid = ptree.beginTrans();
		boolean error = false;
		for (int ii = start ; ii < end && !error; ii++)
		{
			ptree.readData(xid, tnum, ii, buffer);
			if(!ADiskUnit.checkBuffer((byte)ii, buffer))
			{
				System.out.println("Direct block read write failed for block id: " + ii);
				error = true;
			}
		}
		ptree.commitTrans(xid);
		return !error;
	}

	public static int metadata1ReadWriteCheck(PTree ptree) throws IllegalArgumentException, IOException
	{
		TransID xid = null;
		
		
		xid = ptree.beginTrans();
		int tnum = ptree.createTree(xid);
		ptree.commitTrans(xid);
		
		byte[] treeMetaData = new byte[PTree.METADATA_SIZE];
		ADiskUnit.setBuffer((byte)5, treeMetaData);
		xid = ptree.beginTrans();
		ptree.writeTreeMetadata(xid, tnum, treeMetaData);
		ptree.commitTrans(xid);
		
		
		xid = ptree.beginTrans();
		ADiskUnit.setBuffer((byte)0, treeMetaData);
		ptree.readTreeMetadata(xid, tnum, treeMetaData);
		ptree.commitTrans(xid);
		
		if(ADiskUnit.checkBuffer((byte)5, treeMetaData))
			System.out.println("MetaData 1 read/write check - OK");
		else
			System.out.println("MetaData 1 read/write check - FAILED");
		
		return tnum;
	}
	
	public static boolean metadata1ReadCheck(PTree ptree, int tnum) throws IllegalArgumentException, IOException
	{
		TransID xid = null;
		
		
		xid = ptree.beginTrans();
		byte[] treeMetaData = new byte[PTree.METADATA_SIZE];
		ADiskUnit.setBuffer((byte)0, treeMetaData);
		ptree.readTreeMetadata(xid, tnum, treeMetaData);
		ptree.commitTrans(xid);
		
		if(ADiskUnit.checkBuffer((byte)5, treeMetaData))
		{
			System.out.println("MetaData 1 read check - OK");
			return true;
		}
		else
		{
			System.out.println("MetaData 1 read check - FAILED");
			return false;
		}
	}

}
