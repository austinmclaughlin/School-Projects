/*
 * Transaction.java
 *
 * List of active transactions.
 *
 * You must follow the coding standards distributed
 * on the class web page.
 *
 * (C) 2011 Mike Dahlin
 *
 */

import java.io.IOException;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class Transaction {

    //
    // You can modify and add to the interfaces
    //
    public static final int ABORTED = -1;
    public static final int INPROGRESS = 0;
    public static final int COMMITTED = 1;
    public static final int HEADERID = 222;
    public int status;
    public TransID tid;
    public int logStart;
    public int numSectors;
    private Hashtable<Integer, byte[]> data; //not sure is hash tables are the right way to go here

    public Transaction(TransID id) {
        data = new Hashtable<Integer, byte[]>();
        this.status = INPROGRESS;
        this.tid = id;
        logStart = -1;
        numSectors = -1;
    }

    //Check for size of buffer. It must fit within a sector - if not throw illegal argument exception 
    //Check for sector number range. If not valid, throw index out of bounds exception.
    public void addWrite(int sectorNum, byte buffer[])
    	throws IllegalArgumentException, IndexOutOfBoundsException {

    	if(status == ABORTED)
    		return;
    	
    	if (buffer.length > Disk.SECTOR_SIZE) {
            throw new IllegalArgumentException();
        }
        
        if (sectorNum < 0 + Disk.ADISK_REDO_LOG_SECTORS || sectorNum > Disk.NUM_OF_SECTORS) {
            throw new IndexOutOfBoundsException();
        }
        
        //If all checks have been made, smiply add this write to the active transaction hash table
        byte[] cloneOfGivenBuffer =   (byte[])buffer.clone(); 
        data.put(sectorNum, cloneOfGivenBuffer);
    }

    //
    // Return true if this transaction has written the specified
    // sector; in that case update buffer[] with the written value.
    // Return false if this transaction has not written this sector.
    //
    public boolean checkRead(int sectorNum, byte buffer[]) {
    	if(status == ABORTED)
    		return false;
    	
        if (buffer.length > Disk.SECTOR_SIZE) {
            throw new IllegalArgumentException();
        }
        if (sectorNum < 0 + Disk.ADISK_REDO_LOG_SECTORS || sectorNum > Disk.NUM_OF_SECTORS) {
            throw new IndexOutOfBoundsException();
        }
        //The following loop is to copy the data from the data to the buffer. Otherwise buffer does nto change
        byte[] temp = (byte[]) data.get(sectorNum);
        
        if (temp == null) {
            return false;
        }
        
        for(int i = 0 ; i< buffer.length ; i++)
        {
        	buffer[i] = temp[i];	
        }
        return true;
    }

    public void commit()
            throws IOException, IllegalArgumentException {

    	if (data.size() > Common.MAX_WRITES_PER_TRANSACTION){
    		System.out.println("Transaction has too many writes: " + data.size() + "; " +
    				"must be less than " + Common.MAX_WRITES_PER_TRANSACTION);
    		throw new IllegalArgumentException();
    	}
        status = COMMITTED;
    }

    //clean up memory allocated by this transaction
    public void abort()
            throws IOException, IllegalArgumentException {
        status = ABORTED;
        data.clear();
    }

    //
    // These methods help get a transaction from memory to
    // the log (on commit), get a committed transaction's writes
    // (for writeback), and get a transaction from the log
    // to memory (for recovery).
    //
    //
    // For a committed transaction, return a byte
    // array ArrayList that can be written to some number
    // of sectors in the log in order to place
    // this transaction on disk. Note that the
    // first sector is the header, which lists
    // which sectors the transaction updates
    // and the last sector is the commit.
    // The k sectors between should contain
    // the k writes by this transaction.
    //
    public ArrayList<byte[]> getSectorsForLog(){
    	//Status may not be committed at this point
    	//Only after a transaction is written to disk, the status is changed to COMMIT
    	ArrayList<byte[]> result = new ArrayList<byte[]>(data.size() + 2); 
    	byte[] header = new byte[Disk.SECTOR_SIZE];
    	byte[] commit = new byte[Disk.SECTOR_SIZE];
    	
    	//setup header
    	// 0 - HEADERID 
    	// 1 - number of sectors in this transaction (not including header and commit sectors)
    	// 2-5 - transaction id
    	// 6 - (6 + data.size()*4) - sector numbers contained in this transaction
    	// everything after - 0's
    	header[0] = (byte) HEADERID;
    	header[1] = (byte) getNUpdatedSectors();
    	int i = 2;
    	byte[] temp1 = Transaction.intToBytes(tid.id);
    	header[i] = temp1[0];
		header[i+1] = temp1[1];
		header[i+2] = temp1[2];
		header[i+3] = temp1[3];
		i+=4;
    	Iterator<Integer> sectors1= this.getKeys();
    	while (sectors1.hasNext()){
    		byte[] temp2 = intToBytes(sectors1.next());
    		header[i] = temp2[0];
    		header[i+1] = temp2[1];
    		header[i+2] = temp2[2];
    		header[i+3] = temp2[3];
    		i+=4;
    	}
    	for (; i < Disk.SECTOR_SIZE; i++){
    		header[i] = 0;
    	}
    	
    	result.add(header);
    	Iterator<Integer> sectors2 = this.getKeys();
    	while (sectors2.hasNext()){
    		result.add(data.get(sectors2.next()));
    	}
    	result.add(commit);
    	
    	return result;
    }

    //
    // You'll want to remember where a Transactions
    // log record ended up in the log so that
    // you can free this part of the log when
    // writeback is done.
    //
    public void rememberLogSectors(int start, int nSectors) {
        logStart = start;
        numSectors = nSectors;
    }

    public int recallLogSectorStart() {
        return logStart;
    }

    public int recallLogSectorNSectors() {
        return numSectors;
    }

    //
    // For a committed transaction, return
    // the number of sectors that this
    // transaction updates. Used for writeback.
    //
    public int getNUpdatedSectors() {
        return data.size(); //what about header and commit?
    }

    //
    // For a committed transaction, return
    // the sector number and body of the
    // ith sector to be updated. Return
    // a secNum and put the body of the
    // write in byte array. Used for writeback.
    //
    public int getUpdateI(int i, byte buffer[]) {
        return -1;
        
    }

    //
    // Parse a sector from the log that *may*
    // be a transaction header. If so, return
    // the total number of sectors in the
    // log that should be read (including the
    // header and commit) to get the full transaction.
    //
    public static int parseHeader(byte buffer[]) {
        return -1;
    }

    //
    // Parse k+2 sectors from disk (header + k update sectors + commit).
    // If this is a committed transaction, construct a
    // committed transaction and return it. Otherwise
    // throw an exception or return null.
    public static Transaction parseLogBytes(byte buffer[]) {
        return null;
    }
    

    
	public static byte[] intToBytes(int value) {
		return new byte[] { (byte) (value >>> 24), (byte) (value >>> 16),
				(byte) (value >>> 8), (byte) value };
	}
	
	// takes an array of size 4 and converts it to an int
	public static int bytesToInt(byte [] b) {
		int value = 0;
        for (int i = 0; i < 4; i++) {
            int shift = (4 - 1 - i) * 8;
            value += (b[i] & 0x000000FF) << shift;
        }
        return value;
		}

	public Iterator<Integer> getKeys() {
		return data.keySet().iterator();
	}

	public byte[] getData(int sector) {
		return data.get(sector);
	}
}
