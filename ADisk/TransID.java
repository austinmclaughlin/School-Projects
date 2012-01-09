/*
 * TransId.java
 *
 * Interface to ADisk
 *
 * You must follow the coding standards distributed
 * on the class web page.
 *
 * (C) 2007 Mike Dahlin
 *
 */
public class TransID{
	
	private static int maxID = 1;
	public int id;
	
	public TransID(){
		maxID++;
		id = maxID;
	}
	
	public TransID(int tid){
		id = tid;
		if (maxID < tid)
			maxID = tid;
	}


}
