
public class TransactionTests {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		//----------------------------------------------------------
		//tests for TransID, Transaction, and ActiveTransactionList
		//----------------------------------------------------------
		
		ActiveTransactionList aList = new ActiveTransactionList();

		TransID id1 = new TransID();
		Transaction trans1 = new Transaction(id1);
		aList.put(id1, trans1);

                TransID id2 = new TransID();
		Transaction trans2 = new Transaction(id2);
		aList.put(id2, trans2);

                TransID id3 = new TransID();
		Transaction trans3 = new Transaction(id3);
		aList.put(id3, trans3);
		
		
		
		System.out.println(id1.id);
		System.out.println(id2.id);
		System.out.println(id3.id);
		

	}

}
