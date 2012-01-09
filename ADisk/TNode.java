/*
 * CallbackTracker.java
 *
 * Wait for a particular tag to finish...
 *
 * You must follow the coding standards distributed
 * on the class web page.
 *
 *
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


public class TNode {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	byte[] metadata1;
	byte[] metadata2;
	int[] directBlockPointers;
	int[] indirectNodePointers;
	int[] doubleIndirectNodePointers;

	public TNode()
	{
		allocateSpace();
	}

	
	private void allocateSpace() {
		metadata1 = new byte[PTree.METADATA_SIZE];
		metadata2 = new byte[PTree.METADATA_SIZE];
		directBlockPointers = new int[PTree.TNODE_DIRECT];
		indirectNodePointers = new int[PTree.TNODE_INDIRECT];
		doubleIndirectNodePointers = new int[PTree.TNODE_DOUBLE_INDIRECT];
	}
	
	//data blocks should be cleaned up before calling deleteTNode 
	public void deleteTNode(){
		metadata1 = null;
		metadata2 = null;
		directBlockPointers = null;
		indirectNodePointers = null;
		doubleIndirectNodePointers = null;
	}

	public void readObject(ObjectInputStream aInputStream)
	throws ClassNotFoundException, IOException {

		allocateSpace();
		aInputStream.read(metadata1);
		aInputStream.read(metadata2);
		for(int ii = 0 ; ii < directBlockPointers.length ; ii++)
			directBlockPointers[ii] = aInputStream.readInt(); 
		for(int ii = 0 ; ii < indirectNodePointers.length ; ii++)
			indirectNodePointers[ii] = aInputStream.readInt();
		for(int ii = 0 ; ii < doubleIndirectNodePointers.length ; ii++)
			doubleIndirectNodePointers[ii] = aInputStream.readInt();
		
}

	/**
	 * This is the default implementation of writeObject.
	 * Customize if necessary.
	 */
	public void writeObject(
			ObjectOutputStream aOutputStream
	) throws IOException 
	{
		aOutputStream.write(metadata1);
		aOutputStream.write(metadata2);
		for(int ii = 0 ; ii < directBlockPointers.length ; ii++)
			aOutputStream.writeInt(directBlockPointers[ii]);
		for(int ii = 0 ; ii < indirectNodePointers.length ; ii++)
			aOutputStream.writeInt(indirectNodePointers[ii]);
		for(int ii = 0 ; ii < doubleIndirectNodePointers.length ; ii++)
			aOutputStream.writeInt(doubleIndirectNodePointers[ii]);

		// //perform the default serialization for all non-transient, non-static fields
		//aOutputStream.defaultWriteObject();
	}
	


}