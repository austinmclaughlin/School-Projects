import java.io.IOException;

public class BulkReadWriteTest extends Thread{
	
	private ADisk adisk;
	private byte uniqueNumberToThread;
	public Transaction prev;
	
	public BulkReadWriteTest(ADisk _adisk, byte seed) {
		adisk = _adisk;
		prev = null;
		uniqueNumberToThread = 0;
	}
	
	public void run() {
		try {
			byte writeByteBuffer[] = new byte[Disk.SECTOR_SIZE];
			byte readByteBuffer[] = new byte[Disk.SECTOR_SIZE];
			boolean error = false;
			
			TransID tid = adisk.beginTransaction();
			int[] locationCount =  new int[4];
			
			for(int ii=  0 ; ii < 32 && !error ; ii++)
			{
				ADiskUnit.setBuffer((byte)(ii + uniqueNumberToThread), writeByteBuffer);
				adisk.writeSector(tid, Disk.ADISK_REDO_LOG_SECTORS + 1 + ii , writeByteBuffer);

				//The following read should simply look up a previous entry in the active transaction list
				int location = adisk.readSector(tid, Disk.ADISK_REDO_LOG_SECTORS + 1 + ii, readByteBuffer);
				locationCount[location]++;
				if(!ADiskUnit.checkBuffer((byte)(ii + uniqueNumberToThread), readByteBuffer))
				{
					error = true;
					System.out.println(locationTranslate(location) + " read failed: location: " + (Disk.ADISK_REDO_LOG_SECTORS + 1 + ii ) + " Data expected: " + ii);
				}
				
				ADiskUnit.setBuffer((byte)0, writeByteBuffer);
				ADiskUnit.setBuffer((byte)0, readByteBuffer);
			}
			
			if(!error)
				System.out.println("ATL read write check- OK");
			
			//Once it is committed, stuff to go to the writeBackList and the log. There is no way
			//i can prevent it from getting immediately transferred to the disk.
			//The error message is either that of the disk or the writeBackList
			
			try
			{
				adisk.wbThread.addTransactionWatch(tid);
				adisk.commitTransaction(tid);
				adisk.wbThread.waitForTransaction(tid);			
				error = false;
				for( int ii = 0 ; ii < 32 && !error ; ii++ )
				{
					//The following read should simply look up a previous entry in the active transaction list
					int location = adisk.readSector(tid, Disk.ADISK_REDO_LOG_SECTORS + 1 + ii, readByteBuffer);
					locationCount[location]++;
					if(!ADiskUnit.checkBuffer((byte)ii,readByteBuffer))
					{
						error = true;
						
						System.out.println(locationTranslate(location) + " read failed: location: " + (Disk.ADISK_REDO_LOG_SECTORS + 1 + ii ) + " Data expected: " + ii);
					}
					
					ADiskUnit.setBuffer((byte)0, writeByteBuffer);
					ADiskUnit.setBuffer((byte)0, readByteBuffer);
				}
				
				System.out.println("ATL Reads: " + locationCount[1]);
				System.out.println("WBL Reads: " + locationCount[2]);
				System.out.println("Disk Reads: " + locationCount[3]);
				
				if(!error)
					System.out.println("WBL/Disk read write check- OK");
			}
			catch(Exception e)
			{
				System.out.println("Transaction commit failed. Disk is dead");
			}
			
		
		} catch (IndexOutOfBoundsException e) {
			System.out.println("IndexOutOfBoundsException occured. What do I do?");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("IOException occured. What do I do?");
			e.printStackTrace();
		}
	}
	public static String locationTranslate(int location)
	{
		String retString; 
		switch (location)
		{
			case 1:
				retString = "ATL";
				break;
			case 2:
				retString = "WBL";
				break;
			case 3:
				retString = "Disk";
				break;
			default:
				retString = "Invalid location";
				break;
		}
		return retString;
	}
}
