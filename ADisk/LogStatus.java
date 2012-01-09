import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

/*
 * LogStatus.java
 *
 * Keep track of where head of log is (where should next
 * committed transaction go); where tail is (where
 * has write-back gotten); and where recovery
 * should start.
 *
 * You must follow the coding standards distributed
 * on the class web page.
 *
 * (C) 2011 Mike Dahlin
 *
 */
public class LogStatus{

	public static final int INPROGRESS = 34234;
	public static final int OK = 9083;

	private SimpleLock lock;
	private ADisk ad;
	private int head;
	private int tail;
	private int checkpoint;
	
	public int useStatus;
	
	int[] log;
	
	public LogStatus(ADisk adisk){
		log = new int[Disk.ADISK_REDO_LOG_SECTORS];
		for (int i : log){
			log[i] = OK;
		}
		this.head = 1;
		this.tail = 1;
		this.checkpoint = 1;
		this.useStatus = OK;
		this.ad = adisk;
		this.lock = new SimpleLock();
	}
	
    // 
    // Return the index of the log sector where
    // the next transaction should go.
    //
    public int reserveLogSectors(int nSectors)
    {
        return head;
    }

    //
    // The write back for the specified range of
    // sectors is done. These sectors may be safely 
    // reused for future transactions. (Circular log)
    //
    public int writeBackDone(int startSector, int nSectors)
    {
        return -1;
    }

    //
    // During recovery, we need to initialize the
    // LogStatus information with the sectors 
    // in the log that are in-use by committed
    // transactions with pending write-backs
    //
    public void recoverySectorsInUse(int startSector, int nSectors)
    {
    }

    //
    // On recovery, find out where to start reading
    // log from. LogStatus should reserve a sector
    // in a well-known location. (Like the log, this sector
    // should be "invisible" to everything above the
    // ADisk interface.) You should update this
    // on-disk information at appropriate times.
    // Then, on recovery, you can read this information 
    // to find out where to start processing the log from.
    //
    // NOTE: You can update this on-disk info
    // when you finish write-back for a transaction. 
    // But, you don't need to keep this on-disk
    // sector exactly in sync with the tail
    // of the log. It can point to a transaction
    // whose write-back is complete (there will
    // be a bit of repeated work on recovery, but
    // not a big deal.) On the other hand, you must
    // make sure of three things: (1) it should always 
    // point to a valid header record; (2) if a 
    // transaction T's write back is not complete,
    // it should point to a point no later than T's
    // header; (3) reserveLogSectors must block
    // until the on-disk log-start-point points past
    // the sectors about to be reserved/reused.
    //
    public int logStartPoint(){
    	//you can also return checkpoint position. 
    	//As far as this design is concerned, they are both the same
        return tail;
    }

	public void writeLogHeader(int headPosition, int tailOffset, int dummyCheckpointOffset) {
		try
		{
			lock.lock();
			byte[] header = new byte[Disk.SECTOR_SIZE];
			byte[] temp;
			if(headPosition == -1)
				headPosition = this.head;
			int tailPosition = this.tail + tailOffset;
			
			if(tailPosition > ADisk.REDO_LOG_SECTORS)
			{
				tailPosition = tailPosition - ADisk.REDO_LOG_SECTORS + 1;
			}
			
			int checkpointPosition = tailPosition;
			
					
			if(tailPosition == -1)
				tailPosition = this.tail;
			
			if(checkpointPosition == -1)
				checkpointPosition = this.checkpoint;
			
			int i = 0;

			temp = Transaction.intToBytes(headPosition);
			System.arraycopy(temp, 0, header, 0, 4);
			i+=4;
			
			temp = Transaction.intToBytes(tailPosition);
			System.arraycopy(temp, 0, header, i, 4);
			i+=4;
			
			temp = Transaction.intToBytes(checkpointPosition);
			System.arraycopy(temp, 0, header, i, 4);
			i+=4;
			
	    	for (; i < Disk.SECTOR_SIZE; i++){
	    		header[i] = 0;
	    	}
	
	    	
	        ad.tracker.resetNumLogWritesCompleted();
	        
	        //Now write
	        ad.write(CallbackTracker.LOG_TAG, 0, header);
	        
	        //Now wait for all these writes to get done.
	        //Since we are going to be writing only one sector, number of writes we need to wait for is 1
	        ad.tracker.logWriteCompletionWait(1);
	        
	    	this.head = headPosition;
	    	this.tail = tailPosition;
	    	this.checkpoint = checkpointPosition;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			lock.unlock();
		}
	}
    
	public int[] readLogHeader() throws IllegalArgumentException, IOException {
		try
		{
			lock.lock();
			byte[]  header = new byte[Disk.SECTOR_SIZE];
			
			ad.read(0, header);
			
			byte[] temp = new byte[4];
			System.arraycopy(header, 0, temp, 0, 4);
			this.head = Transaction.bytesToInt(temp);
			
			System.arraycopy(header, 4, temp, 0, 4);
			this.tail = Transaction.bytesToInt(temp);
			
			System.arraycopy(header, 8, temp, 0, 4);
			this.checkpoint = Transaction.bytesToInt(temp);
			
			return new int[] {head, tail, checkpoint};

		}
		finally
		{
			lock.unlock();
		}
	}

    public int writeToLog(Transaction t) throws IOException
    {
    	try
    	{
    		//Step 0: Lock
	    	lock.lock();

	    	assert(this.useStatus == LogStatus.OK);
	        this.useStatus = LogStatus.INPROGRESS;
 
	        //Step 2: Write to log and wait for its completion
	        ArrayList<byte[]> logSec = t.getSectorsForLog();
	        int logLength = logSec.size();
	        int writePosition = this.reserveLogSectors(t.getNUpdatedSectors() + 2);
	        //callBackTracker keeps count of the number of requestsDone
	        //with the tag LOG_TAG. This count has to be reset to 0
	        ad.tracker.resetNumLogWritesCompleted();
	        
	        //Now write
	        Iterator itr = logSec.iterator();
	        while( itr.hasNext() ){
	        	ad.write(CallbackTracker.LOG_TAG, writePosition, (byte[])itr.next());
	        	writePosition++;
	        	
	        	//Implementation of circular log
	        	if(writePosition > Disk.ADISK_REDO_LOG_SECTORS )
	        		writePosition = 1; //0th sector is reserved for header
	        }
	        
	        //Now wait for all these writes to get done.
	        ad.tracker.logWriteCompletionWait(logLength);
	        

	        //Update The disk log's header (not the transaction's header)
	        //about the change in the head position
	        //logStatus variable is also updated by this function
	        this.writeLogHeader(writePosition ,0,0);
	        
	        
	        
//	        //TODO: Set read statements to confirm that the log has been written and the header has been updated
//			byte[]  header = new byte[Disk.SECTOR_SIZE];
//			ad.read(0, header);
//			byte[] temp = new byte[4];
//			System.arraycopy(header, 0, temp, 0, 4);
//			if()
//			System.arraycopy(header, 4, temp, 0, 4);
//			System.arraycopy(header, 8, temp, 0, 4);
//						
			
	        
	        
	        //This ends the confirmation that commit has been done
	        return writePosition;

    	} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
    	}
    	finally
    	{
	        this.useStatus = LogStatus.OK;
    		lock.unlock();
    	}
		return -1;
    }
}