import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;


public class WriteBackThread extends Thread{
	
	private ADisk ad;
	public Transaction prev;
	
	//This is for tracking reads
	private SimpleLock lock;
	private Condition writebackDone;
	private LinkedList<TransID> transactionsReceived;
	private LinkedList<TransID> waitTransactionsList;
	
	public WriteBackThread(ADisk adisk) {
		ad = adisk;
		prev = null;
		lock = new SimpleLock();
		writebackDone = lock.newCondition();
		transactionsReceived = new LinkedList<TransID>();
		waitTransactionsList = new LinkedList<TransID>();
	}
	
	public void run() {
		try
		{
			while(true){
				Transaction t = ad.wList.getNextWriteback();
				int transactionLength = 0;
				t = ad.wList.getNextWriteback();
				assert (t != null);
				Iterator<Integer> sectors = t.getKeys();
				
				//The tracker must be reset before starting to write a transaction
		        ad.tracker.resetNumTransactionWritesCompleted();
		        
		        //Write the stuff to the disk
				while (sectors.hasNext()){
					int sector = sectors.next();
					ad.write(CallbackTracker.DISK_WRITE_TAG, sector, t.getData(sector));
					transactionLength++;
				}
				
				//Now wait for all these writes to get done.
		        ad.tracker.transactionWriteCompletionWait(transactionLength);
		        int tailOffset = transactionLength + 2;
		        ad.logStatus.writeLogHeader(-1, tailOffset,0);
       
		        
		        //System.out.println("Transaction " + t.tid.id + " written to disk." );
				
				//Once all the writes are completed, remove the item from the writebacklist.
		        prev = ad.wList.removeNextWriteback();
		        
		        
				if(waitTransactionsList.contains(t.tid))
				{
					waitTransactionsList.removeFirstOccurrence(t.tid);
					transactionsReceived.add(t.tid);
					transactionReceivedSignal();
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();	
		}
		finally
		{
		}
		
	}
	
	private void transactionReceivedSignal()
	{
		try
		{
			lock.lock();
			writebackDone.signalAll();
		}
		finally
		{
			lock.unlock();
		}
	}
	public void addTransactionWatch(TransID tid)
	{
		waitTransactionsList.add(tid);
	}
	public void waitForTransaction(TransID tid){
		try
		{
			lock.lock();
			while(!transactionsReceived.contains(tid))
			{
				writebackDone.awaitUninterruptibly();
				int i = 1; 
				i=i+1;
			}
			
			assert(transactionsReceived.contains(tid) );
			
			transactionsReceived.removeFirstOccurrence(tid);
		}
		finally
		{
			lock.unlock();
		}
		
	}
		
}
