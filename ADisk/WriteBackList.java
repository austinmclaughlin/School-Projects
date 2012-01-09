
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;

/*
 * WriteBackList.java
 *
 * List of commited transactions with pending writebacks.
 *
 * You must follow the coding standards distributed
 * on the class web page.
 *
 * (C) 2011 Mike Dahlin
 *
 */
public class WriteBackList{

    // 
    // You can modify and add to the interfaces
    //

    private Queue<Transaction> wList;
    private SimpleLock lock;
    private Condition listHasElements;
    
    public WriteBackList(){
    	lock = new SimpleLock();
        wList = new LinkedList<Transaction>();
        listHasElements = lock.newCondition();
    }

    // Once a transaction is committed in the log,
    // move it from the ActiveTransactionList to 
    // the WriteBackList
    public void addCommitted(Transaction t){
    	try
    	{
    		lock.lock();
            wList.offer(t);
            listHasElements.signalAll();
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

    //
    // A write-back thread should process
    // writebacks in FIFO order.
    //
    // NOTE: Don't remove the Transaction from
    // the list until the writeback is done
    // (reads need to see them)!
    //
    // NOTE: Service transactions in FIFO order
    // so that if there are multiple writes
    // to the same sector, the write that is
    // part of the last-committed transaction "wins".
    //
    // NOTE: you need to use log order for commit
    // order -- the transaction IDs are assigned
    // when transactions are created, so commit
    // order may not match transaction ID order.
    //    
    public Transaction getNextWriteback(){
    	try
    	{
	    	lock.lock();
	    	while(wList.isEmpty())
	    	{
	    		listHasElements.awaitUninterruptibly();
	    	}
	        assert(!wList.isEmpty());
	        Transaction t = wList.peek();
	        
            return t;
    	} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        finally{
        	lock.unlock();
        }
		return null;
    }

    //
    // Remove a transaction -- its writebacks
    // are now safely on disk.
    //
    public Transaction removeNextWriteback(){
    	try
    	{
	    	lock.lock();
	    	while(wList.isEmpty())
	    	{
	    		listHasElements.awaitUninterruptibly();
	    	}
	        assert(!wList.isEmpty());
	        Transaction t =  wList.poll();
            return t;
    	} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        finally{
        	lock.unlock();
        }
		return null;
    }

    //
    // Check to see if a sector has been written
    // by a committed transaction. If there
    // are multiple writes to the same sector,
    // be sure to return the last-committed write.
    //
    public boolean checkRead(int secNum, byte buffer[])
    throws IllegalArgumentException, 
           IndexOutOfBoundsException
    {
    	try
    	{
    		lock.lock();
        	//Iterate through each element of the list, whichever transaction that has secNum will write it's data to buffer[]
        	//assumes buffer[] will not be accessed until checkRead returns
        	Iterator<Transaction> itr = wList.iterator();
        	boolean result = false;
        	Transaction t;
        	while (itr.hasNext()){
        		t = itr.next();
        		if (t.getData(secNum) != null){
        			
        			//The following loop is to copy the data from the data to the buffer. Otherwise buffer does nto change
        	        byte[] temp = (byte[]) t.getData(secNum);
        	        
        	        if (temp == null) {
        	            result = false;
        	        }
        	        else
        	        {
        	        	System.arraycopy(temp, 0, buffer, 0, buffer.length);	
    	    	        result = true;
        	        }
        		}
        	}
            return result;
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    	finally
    	{
    		lock.unlock();
    	}
		return false;
    }

    
    
}
