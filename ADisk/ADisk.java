/*
 * ADisk.java
 *
 * You must follow the coding standards distributed
 * on the class web page.
 *
 * (C) 2007, 2010 Mike Dahlin
 *
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

//import writebackthread.WriteBackThread;

public class ADisk {

	//internal data structures
    public Disk disk;
    public ActiveTransactionList aList;
    public WriteBackList wList;
    public LogStatus logStatus;
    public CallbackTracker tracker;
    public int[] status;
    
    //thread that watches the WriteBackList, writes data to disk when there are available transactions to be written
    public WriteBackThread wbThread;
    
    //locks and condition variables for ADisk
    private SimpleLock lock;
    
    //-------------------------------------------------------
    // The size of the Redo log in sectors
    //-------------------------------------------------------
    public static final int REDO_LOG_SECTORS = 1024;
    
    //-------------------------------------------------------
    // Status indicators for sectors
    //-------------------------------------------------------
    public static final int INPROGRESS = 234;
    public static final int DONE_OK = 9234;
    public static final int DONE_ERROR = 22842;


    //-------------------------------------------------------
    //
    // Allocate an ADisk that stores its data using
    // a Disk.
    //
    // If format is true, wipe the current disk
    // and initialize data structures for an empty
    // disk.
    //
    // Otherwise, initialize internal state, read the log,
    // and redo any committed transactions.
    //
    //-------------------------------------------------------
    public ADisk(boolean format) throws IllegalArgumentException, IOException {
        try {
        	//Initilize the lock 
        	lock = new SimpleLock();
        	
        	//Initialize the active transaction list
        	aList = new ActiveTransactionList();
	        
        	//Initialize the writeBackList
        	wList = new WriteBackList();
	        
        	//Initialize the log status class
	        logStatus = new LogStatus(this);
	        
	        //Create a variable to store the status of each sector
	        //Ideally I would expect disk workers to take care of this
	        //When adisk sends a write, it sets the sector to busy
	        status = new int[Disk.NUM_OF_SECTORS];
	        
	        for(int i = 0 ; i < status.length ; i++)
	        {
	        	status[i] = DONE_OK;
	        }

	        //Once the callback tracker knows that the secor has been 
	        //written to, it changes it back to ready
	        //In the mean time, writes and reads dont occur in the sector
	        tracker = new CallbackTracker(status);
	        
	        //Create a disk with the above tracker
            disk = new Disk(tracker);

            //Create a thread to do the writeback to the disk
	        wbThread = new WriteBackThread(this);
	        
	        //Start the thread
	        wbThread.start();
	        
	        //Now given the input by the user, format the disk / recover form the logs of the disk
	        if (format) {
	            format();
	        } else {
	            recover();
	        }
        } catch (FileNotFoundException ex) {
            System.out.println("ADisk: Unable to create new Disk");
            System.exit(-1);
        }
        catch (Exception e)
        {
        	System.out.println(e.toString());
        	System.out.println(e.getMessage());
        }
        finally
        {
        	System.out.println("ADisk contructed successfully");
        }
    }

	// -----------------------------------------------------------------------
    //
    // called by ADisk constructor, attempts to recover transactions from the log
    // after a failure
    //
    // 0th sector of disk contains stored header and tail information
    // log is located on sectors 1 to Disk.REDO_LOG_SECTORS
    // The log stores Transactions contiguously. Each Transaction starts with a header
    // sector which stores metadata about itself, followed by sectors of data 
    // (there will be less than Common.MAX_WRITES_PER_TRANSACTION sectors of data),
    // finally the transaction ends with a commit sector
    //
    // -----------------------------------------------------------------------
    private void recover() {
    	try {
    		lock.lock();
    		System.out.println("Recovering from disk");
    		// first read sector 0 and extract head and tail information
    		int[] logPoints = logStatus.readLogHeader();
    		byte[] b = new byte[Disk.SECTOR_SIZE];
    		int head = logPoints[0];
    		int tail = logPoints[1];
    		Queue<Integer> sectors = new LinkedList<Integer>();

    		for (int i = tail ; ; i++)
    		{
    			if(i> ADisk.REDO_LOG_SECTORS)
    				i = 1;

    			if(i == head)
    				break;

    			this.read(i, b);
    	    	//setup header
    	    	// 0 - HEADERID 
    	    	// 1 - number of sectors in this transaction (not including header and commit sectors)
    	    	// 2-5 - transaction id
    	    	// 6 - (6 + data.size()*4) - sector numbers contained in this transaction
    			if (b[0] != (byte)222){
    				System.out.println("Read sector was not a header");
    				break;
    			}
    			
    			int numSectors = (int)b[1];
    			byte[] temp = new byte[4];
    			System.arraycopy(b, 2, temp, 0, 4);
    			
    			int tid = Transaction.bytesToInt(temp);
    			TransID id = new TransID(tid);
    			Transaction t = new Transaction(id);
    			
    			for (int j = 6; j < 6 + numSectors*4; j+=4){
    				System.arraycopy(b, j, temp, 0, 4);
    				sectors.offer(Transaction.bytesToInt(temp));
    			}
    			
    			t.numSectors = numSectors;
    			
    			Iterator<Integer> itr = sectors.iterator();
    			
    			while(itr.hasNext())
    			{
    				i++;
    				if(i > ADisk.REDO_LOG_SECTORS)
    					i = 1;
    				
    				temp = new byte[Disk.SECTOR_SIZE];
    				this.read(i, temp);
    				int num = itr.next();
    				t.addWrite(num, temp);
    			}

    			wList.addCommitted(t);
    			//Just skip the commit block
    			i++;
    		}
    	} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
    		lock.unlock();
    	}
    	
	}

    // -----------------------------------------------------------------------
    //
    // called by ADisk constructor, writes 0's to every sector in the disk
    //
    // -----------------------------------------------------------------------
	private void format() throws IllegalArgumentException, IOException {
        try {
        	System.out.println("Formatting disk");
        	lock.lock();
            byte[] b = new byte[Disk.SECTOR_SIZE];
            for (int i = 0; i < b.length; i++) {
                b[i] = 0;
            }
        	tracker.resetNumFormatWritesCompleted();
            for (int i = 0; i < Disk.NUM_OF_SECTORS; i++) {
                //make sure another thread isn't updating this sector, need a condition variable
                //also need a way to keep track of statuses of sectors i think
            	
                write(CallbackTracker.FORMAT_TAG, i, b);
            }
            tracker.formatWriteCompletionWait(Disk.NUM_OF_SECTORS);
            System.out.println("Formatting completed");
        } finally {
            lock.unlock();
        }
    }

    //-------------------------------------------------------
    //
    // Return the total number of data sectors that
    // can be used *not including space reserved for
    // the log or other data structures*. This
    // number will be smaller than Disk.NUM_OF_SECTORS.
    //
    //-------------------------------------------------------
    public int getNSectors() {
        return Disk.NUM_OF_SECTORS - Disk.ADISK_REDO_LOG_SECTORS; //FIXME
    }

    //-------------------------------------------------------
    //
    // Begin a new transaction and return a transaction ID
    //
    //-------------------------------------------------------
    public TransID beginTransaction() {
        TransID id = new TransID();
        Transaction trans = new Transaction(id);
        aList.put(id, trans);
        return id;
    }

    //-------------------------------------------------------
    //
    // First issue writes to put all of the transaction's
    // writes in the log.
    //
    // Then issue a barrier to the Disk's write queue.
    //
    // Then, mark the log to indicate that the specified
    // transaction has been committed.
    //
    // Then wait until the "commit" is safely on disk
    // (in the log).
    //
    // Then take some action to make sure that eventually
    // the updates in the log make it to their final
    // location on disk. Do not wait for these writes
    // to occur. These writes should be asynchronous.
    //
    // Note: You must ensure that (a) all writes in
    // the transaction are in the log *before* the
    // commit record is in the log and (b) the commit
    // record is in the log before this method returns.
    //
    // Throws
    // IOException if the disk fails to complete
    // the commit or the log is full.
    //
    // IllegalArgumentException if tid does not refer
    // to an active transaction.
    //
    //-------------------------------------------------------
    public void commitTransaction(TransID tid)
            throws IOException{
        //need to remove TID from tList
        //add transaction to the log
        //once transaction is on the log, add to writeBackList
		try {
	        Transaction t = aList.get(tid);

	        //Write to the log
	        int newWritePosition = logStatus.writeToLog(t);
	        
	        if(newWritePosition == -1)
	        	throw new IOException("Disk is dead");
	        
	        //Set status of transaction to committed
	        t.commit();

	        //Move the stuff to the write back list
	        wList.addCommitted(t);
	        
	        //Remove it from the active transaction list
	        aList.remove(tid);
		}
		finally {
			//Should i do this?
	        //logStatus.useStatus = logStatus.OK;
	        //logWrite.signal();
		}
    }

    
	//-------------------------------------------------------
    //
    // Free up the resources for this transaction without
    // committing any of the writes.
    //
    // Throws
    // IllegalArgumentException if tid does not refer
    // to an active transaction.
    //
    //-------------------------------------------------------
    public void abortTransaction(TransID tid)
            throws IllegalArgumentException, IOException {
        Transaction removed = aList.remove(tid);
        removed.abort();
    }

    //-------------------------------------------------------
    //
    // Read the disk sector numbered sectorNum and place
    // the result in buffer. Note: the result of a read of a
    // sector must reflect the results of all previously
    // committed writes as well as any uncommitted writes
    // from the transaction tid. The read must not
    // reflect any writes from other active transactions
    // or writes from aborted transactions.
    //
    // Throws
    // IOException if the disk fails to complete
    // the read.
    //
    // IllegalArgumentException if tid does not refer
    // to an active transaction or buffer is too small
    // to hold a sector.
    //
    // IndexOutOfBoundsException if sectorNum is not
    // a valid sector number
    //
    //Return value 
    // 1 => buffer read from active transaction list
    // 2 => buffer read from writeBackList
    // 3 => buffer read from disk
    //-------------------------------------------------------
    public int readSector(TransID tid, int sectorNum, byte buffer[])
            throws IOException, IndexOutOfBoundsException {
        //check uncommitted transactions with matching tid
        //if not found, check committed transactions for sectorNum
        //if still not found, read from the log and finally the disk
    	Transaction thisTransaction;
    	
    	if(sectorNum< 0 + Disk.ADISK_REDO_LOG_SECTORS || sectorNum > Disk.NUM_OF_SECTORS)
    		throw new IndexOutOfBoundsException();
    	
    	//Check the active transaction list
    	thisTransaction = aList.get(tid);

    	if(thisTransaction != null)
    		if(thisTransaction.checkRead(sectorNum, buffer))
    			return 1;
    	
    	//Check the WriteBackList
    	if(wList.checkRead(sectorNum, buffer))
    		return 2;

    	//If there is one more request for a read to this same sector, then we will have to wait
    	//until that read completes and then only issue our request
    	tracker.waitForReadSector(sectorNum);
    	disk.startRequest(Disk.READ, CallbackTracker.READ_TAG, sectorNum, buffer);
    	tracker.readCompletionWait(sectorNum);
    	
    	return 3;
    }

    //-------------------------------------------------------
    //
    // Buffer the specified update as part of the in-memory
    // state of the specified transaction. Don't write
    // anything to disk yet.
    //
    // Concurrency: The final value of a sector
    // must be the value written by the transaction that
    // commits the latest.
    //
    // Throws
    // IllegalArgumentException if tid does not refer
    // to an active transaction or buffer is too small
    // to hold a sector.
    //
    // IndexOutOfBoundsException if sectorNum is not
    // a valid sector number
    //
    //-------------------------------------------------------
    public void writeSector(TransID tid, int sectorNum, byte buffer[])
            throws IllegalArgumentException,
            IndexOutOfBoundsException {
    	
    	if(sectorNum< 0 + Disk.ADISK_REDO_LOG_SECTORS || sectorNum > Disk.NUM_OF_SECTORS)
    		throw new IndexOutOfBoundsException();
    	
    	Transaction thisTransaction = aList.get(tid);
    	
    	if(thisTransaction == null)
    		throw new IllegalArgumentException();
    	
    	thisTransaction.addWrite(sectorNum, buffer);
    }

    public void write(int tag, int sectorNum, byte[] b) 
    throws IllegalArgumentException, IOException {
        try {
            lock.lock();
            
            //Check for the status of the sector and wait for it to become free
            while (status[sectorNum] == INPROGRESS) {
                tracker.waitForSector();
            }
            assert status[sectorNum] != INPROGRESS;
            
            //Set the status to INPROGRESS
            status[sectorNum] = INPROGRESS;
            
            disk.startRequest(Disk.WRITE, tag, sectorNum, b);// might want to catch these exceptions
           
        } finally {
            lock.unlock();
        }
    }
    
    public void read(int sectorNum, byte[] b) throws IllegalArgumentException, IOException {
    	try {
    		lock.lock();
            //Check for the status of the sector and wait for it to become free
            while (status[sectorNum] == INPROGRESS) {
                tracker.waitForSector();
            }
            assert status[sectorNum] != INPROGRESS;
            
        	tracker.waitForReadSector(sectorNum);
        	disk.startRequest(Disk.READ, CallbackTracker.READ_TAG, sectorNum, b);
        	tracker.readCompletionWait(sectorNum);
 		
    	} finally {
    		lock.unlock();
    	}
    }

}
