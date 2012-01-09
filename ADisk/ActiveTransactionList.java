
import java.util.Hashtable;

/*
 * ActiveTransaction.java
 *
 * List of active transactions.
 *
 * You must follow the coding standards distributed
 * on the class web page.
 *
 * (C) 2011 Mike Dahlin
 *
 */
public class ActiveTransactionList {

    /*
     * Hash table used to keep track of 
     */
    private Hashtable<TransID, Transaction> table;
    private SimpleLock lock;

    public ActiveTransactionList() {
    	table = new Hashtable<TransID, Transaction>();
    	lock = new SimpleLock();
    }

    public void put(TransID id, Transaction trans) {
    	try{
    		lock.lock();
    		table.put(id, trans);	
    	}
    	finally
    	{
    		lock.unlock();
    	}
    }

    public Transaction get(TransID tid) {
    	try{
    		lock.lock();
    		return table.get(tid);	
    	}
    	finally
    	{
    		lock.unlock();
    	}
        
    }

    public Transaction remove(TransID tid) {
    	try{
    		lock.lock();
    		return table.remove(tid);	
    	}
    	finally
    	{
    		lock.unlock();
    	}
        
    }
}
