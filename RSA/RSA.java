import java.util.*;
import java.io.*;

public class RSA {
	public static void main(String[] Args) throws IOException{
		if(Args[0].equals("key")){
			key(Args[1],Args[2]);
		}
		else if(Args[0].equals("encrypt")){
			encrypt(Args[1],Args[2]);
		}
		else if(Args[0].equals("decrypt")){
			decrypt(Args[1],Args[2]);
        }
	}

    // The key method takes in two large primes and produces
    // a key file containing RSA encryption and decryption keys
    // along with an n, which is p*q. Assumes n will be within the correct range.
	public static void key(String p1, String q1){
		int p = Integer.parseInt(p1);       // convert p1 to an int
		int q = Integer.parseInt(q1);       // convert q1 to an int
		long n = p * q;
		long phiN = (p-1)*(q-1);
		int e = 11;
		long u = e;
		long v = phiN;
		long a = 1, b = 0, c = 0, d = 1;
        // Extended Euclidean Algorithm to find a large
        // relatively prime d.
		while(v != 0){
			long quotient = (long) Math.floor(u/v);
			long u1 = u;
			u = v;
			v = u1 - v * quotient;
			long a1 = a;
			long b1 = b;
			a = c;
			b = d;
			c = a1 - c * quotient;
			d = b1 - d * quotient;
		}
		while(a <= 0)  // if a is negative then keep adding phiN till positive
			a += phiN;
		while(a >= n)
			a -= phiN; // if a is positive subtract from it phiN a random amount of times
		System.out.println(n + " " + e + " " + a); // print out n e and d
	}
    // The method "encrypt" encrypts a file using the RSA algorithm
    // by encoding it with the large primes in the text file provided
	public static void encrypt(String file, String key) throws IOException{
		FileInputStream f = new FileInputStream(file);                 // get file
		BufferedReader k = new BufferedReader(new FileReader(key));    // get key file
		DataInputStream stream = new DataInputStream(f);               // get data from file
		StringTokenizer s = new StringTokenizer(k.readLine());    // read the first line of key file
		long n = Long.parseLong(s.nextToken());                   // get first number and set to n
		long e = Long.parseLong(s.nextToken());                   // get second number and set to e
		byte b = 0;
		byte[] bytes = new byte[3];
		int count = 0;
		DataOutputStream eFile = new DataOutputStream(new FileOutputStream("encrypted"));

        // loop that reads 3 bytes at a time and if the file does not
        // end with 3 bytes then it assigns 0's to the remaining bytes
		while(true){
			b = (byte) stream.read(); // read next byte
			if(b != -1){         // check that its not the end of file
				bytes[count] = b;
				count++;
			}
			else{
				if(count == 1){  // assign zeros to remaining bytes
					bytes[1] = 0;
					bytes[2] = 0;
					eWrite(bytes,n,e, eFile); // send the 3 bytes to be encrypted
				}
				else if(count == 2){ // assign zeros to remaining bytes
					bytes[2] = 0;
					eWrite(bytes,n,e, eFile); // send 3 bytes to be encrypted
				}
				break;
			}
			if(count == 3){
				eWrite(bytes,n,e, eFile); // send the 3 bytes to be encrypted
				count = 0;                // reset count so bytes overwrite byte array
			}
		}
	}

    // The method "eWrite" encrypts a message comprised of 4 bytes using
    // RSA and writes it to a file named "encrypted"
	public static void eWrite(byte[] bytes, long n, long e, DataOutputStream eFile){
		long m = (0x00 << 24)| (bytes[0] << 16) | (bytes[1] << 8) | bytes[2];  // concatenate bytes
		long c = 1;

        // encrypts m so the the new number "c" is m^e mod n.
		while(e > 0){
			if((e & 1) == 1){     // check if rightmost bit is 1
				c = (c * m) % n;
			}
			e = e >> 1;          // shift bits to the right by 1 bit
			m = (m * m) % n;     // compute m^2 mod n
		}
		int cipher = (int) c;    // get rid of extra zeros
		try {
			eFile.writeInt(cipher); // write cipher one byte at a time
		} catch (IOException e1) {
		}
	}

    // The method "decrypt" takes in an encrypted file and decrypts
    // it by reversing the RSA encryption algorithm. It takes the d
    // and n in the key file and finds the original m by computing
    // c^d mod n where c is the cypher text.
	public static void decrypt(String file, String key) throws IOException{
        FileInputStream f = new FileInputStream(file);              // get file
		BufferedReader k = new BufferedReader(new FileReader(key)); // get key file
		DataInputStream stream = new DataInputStream(f);            // get data from file
		StringTokenizer s = new StringTokenizer(k.readLine());      // get first line of key file
		long n = Long.parseLong(s.nextToken());                     // get n
        s.nextToken();                                              // skip e
		long d = Long.parseLong(s.nextToken());                     // get d
		byte b = 0;
		byte[] bytes = new byte[4];
		int count = 0;
        DataOutputStream dFile = new DataOutputStream(new FileOutputStream("decrypted"));

        // read 4 bytes at a time and call dWrite
        // on those 4 bytes
        long numBytes = stream.available();        // check number of bytes
		boolean eof = false;
		while(numBytes != 0){
			b = (byte) stream.read();              // get next byte
			bytes[count] = b;
            numBytes--;                            // decrement number of bytes left to read
            count++;
			if(count == 4){
				if (numBytes == 0)
					eof = true;
				dWrite(bytes, n, d, dFile, eof);        // once you get 4 bytes send to be decrypted; special case for end of file
				count = 0;
			}
		}
		dFile.close();
    }
	
	// The method "dWrite" decrypts a message comprised of 4 bytes using
    // RSA and writes it to a file named "decrypted"
	public static void dWrite(byte[] bytes, long n, long d, DataOutputStream dFile, boolean eof){
        // concatenate bits and mak sure leading 1's on negative numbers dont expand
		long c = ((bytes[0] << 24)&0xFF000000)|((bytes[1] << 16)&0x00FF0000) |
                 ((bytes[2] << 8) & 0x0000FF00) | (bytes[3] & 0x000000FF);
		long m = 1;
        // find original message by using the RSA algorithm.
        // It finds the original message "m" by computing c^d mod n.
        // where c is the cipher text and d is the decryption key.
		while(d > 0){
			if((d & 1) == 1){         // checks right most bit for squaring
				m = (m * c) % n;
			}
			d = d >> 1;               // shift bits of d to the right by 1
			c = (c * c) % n;
		}
		int msg = (int) m;            // remove leading zeros in long
        byte[] b = new byte[3];       // array to hold the bytes to be written
        b[0] = (byte)(msg >> 16);
        b[1] = (byte)(msg >> 8);
        b[2] = (byte) msg;
		try {
			if (!eof)					// not at the end of the file, write all 3 bytes in array
				dFile.write(b,0,3);       // write byte array from left to right
			else {						// end of the file, remove remaining 0's from the end of the byte array before writing
				if (b[1] == 0)
					dFile.write(b,0,1);
				else if (b[2] == 0)
					dFile.write(b,0,2);
				else
					dFile.write(b,0,3);
			}
		} catch (IOException e1) {
		}
    }
}
