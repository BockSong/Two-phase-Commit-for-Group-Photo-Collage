/* 
 *
 * Name: Rong Song
 * Andrew ID: rongsong
 *
 * UserNode.java 
 * 
 * The usernode (client) class for two-phase commit for group photo collage.
 * 
 */

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UserNode implements ProjectLib.MessageHandling {
	public static String myId;
	public static ProjectLib PL;
	private static String log_name = "user.log";
	// map all existing transaction ID with their status (if finished)
	private static ConcurrentHashMap<Integer, Boolean> trans_status = new 
							ConcurrentHashMap<Integer, Boolean>();
	// store <srcPath, trans_ID> pairs for exclusive access to every occupied files
	private static ConcurrentHashMap<String, Integer> file_lock = new 
							ConcurrentHashMap<String, Integer>();

	// lock for read and then modify file lock
	private static Object rw_lock = new Object();
	
	public UserNode( String id ) {
		myId = id;
	}

	/*
	 * get_opinion: get the reponse for a prepare message from the server.
	 */
	private synchronized static String get_opinion(MyMessage mmsg) {
		for (String srcName: mmsg.srcNames) {
			File pic = new File(srcName);

			// check if the image is still there
			if ( !pic.exists() ) {
				return "notok";
			}

			synchronized (rw_lock) {
				// if the image is occupied, return notok
				if (file_lock.containsKey(srcName)) {
					return "notok";
				}
				// lock the involved image
				file_lock.put(srcName, mmsg.trans_ID);
			}

			try {
				// write log for file lock
				BufferedOutputStream writer = new
				BufferedOutputStream(new FileOutputStream(log_name, true));
				
				writer.write(("fileLock\n").getBytes());
				String log_line = Integer.toString(mmsg.trans_ID) + ":" + srcName + "\n";
				writer.write(log_line.getBytes());
				writer.flush();
				writer.close();
				//PL.fsync();
				
			} catch (Exception e) {
				System.out.println( "I/O Error " + e.getMessage());
				e.printStackTrace();
			}
		}

		// check if user agree with that
        String[] arr = new String[mmsg.srcNames.size()]; 
        arr = mmsg.srcNames.toArray(arr); 
		if (PL.askUser(mmsg.img, arr)) {
			return "ok";
		}
		else {
			return "notok";
		}
	}

	/*
	 * deliverMessage: Callback called when message received.
	 * Return: True if the message is handled, or False if not.
	 */
	public synchronized boolean deliverMessage( ProjectLib.Message msg ) {
		String msg_body = new String(msg.body);
		System.out.println( myId + ": Got message " + msg_body );

		MyMessage mmsg = (MyMessage) msg;
		int trans_ID = mmsg.trans_ID;
		ArrayList<String> srcNames = mmsg.srcNames;

		// if it's a prepare message
		if (msg_body.equals("prepare")) {
			// If has handled this message before, skip it
			if (trans_status.containsKey(trans_ID)) {
				System.out.println( myId + ": Do nothing to the resend message. ");
				return true;
			}

			byte[] img = mmsg.img;
			String[] sources = mmsg.sources;

			// init a transaction
			trans_status.put(trans_ID, false);

			try {
				// write log when transaction start
				BufferedOutputStream writer = new
				BufferedOutputStream(new FileOutputStream(log_name, true));
				
				writer.write(("trans\n").getBytes());
				String log_line = Integer.toString(trans_ID) + ":start\n";
				writer.write(log_line.getBytes());
				writer.flush();
				writer.close();
				PL.fsync();
				
			} catch (Exception e) {
				System.out.println( "I/O Error " + e.getMessage());
				e.printStackTrace();
			}
	
			String opinion = get_opinion(mmsg);

			// send the reply
			mmsg = new MyMessage("Server", "opinion".getBytes(), trans_ID, opinion);
			System.out.println( myId + ": Sending opinion of " + opinion );
			PL.sendMessage(mmsg);
		}
		// if it's a decision
		else if (msg_body.equals("decision")) {
			// decision of done
			if (mmsg.decision.equals("done")) {
				// delete sources images from the UserNode directories
				for (String srcName: mmsg.srcNames) {
					File pic = new File(srcName);

					if (!pic.delete()) {
						System.out.println("Error: delete file failed from " + myId + "/" 
																				+ srcName);
					}
					else {
						System.out.println( myId + ": " + srcName + " deleted successfully. " );
					}
				}

				// mark the transaction as finished
				if (trans_status.containsKey(trans_ID)) {
					trans_status.put(trans_ID, true);

					try {
						// write log when transaction start
						BufferedOutputStream writer = new
						BufferedOutputStream(new FileOutputStream(log_name, true));
						
						writer.write(("trans\n").getBytes());
						String log_line = Integer.toString(trans_ID) + ":done\n";
						writer.write(log_line.getBytes());
						writer.flush();
						writer.close();
						//PL.fsync();
						
					} catch (Exception e) {
						System.out.println( "I/O Error " + e.getMessage());
						e.printStackTrace();
					}
				}
				else {
					System.out.println( "Error: cannot find this transaction.");
				}
	
				// unlock occupied resources
				for (Map.Entry<String, Integer> entry : file_lock.entrySet()) {
					if (entry.getValue() == trans_ID) {
						file_lock.remove(entry.getKey());
					}

					try {
						// write log for file lock
						BufferedOutputStream writer = new
						BufferedOutputStream(new FileOutputStream(log_name, true));
						
						writer.write(("fileUnlock\n").getBytes());
						String log_line = Integer.toString(entry.getValue()) + ":"
															 + entry.getKey() + "\n";
						writer.write(log_line.getBytes());
						writer.flush();
						writer.close();
						//PL.fsync();
						
					} catch (Exception e) {
						System.out.println( "I/O Error " + e.getMessage());
						e.printStackTrace();
					}
				}
			}
			// decision of cancel
			else if (mmsg.decision.equals("cancel")) {
				// TODO: should we do it here, or earlier once askUser is false?
				// cancel the transaction
				if (trans_status.containsKey(trans_ID)) {
					trans_status.remove(trans_ID);

					try {
						// write log when transaction start
						BufferedOutputStream writer = new
						BufferedOutputStream(new FileOutputStream(log_name, true));
						
						writer.write(("trans\n").getBytes());
						String log_line = Integer.toString(trans_ID) + ":cancel\n";
						writer.write(log_line.getBytes());
						writer.flush();
						writer.close();
						//PL.fsync();
						
					} catch (Exception e) {
						System.out.println( "I/O Error " + e.getMessage());
						e.printStackTrace();
					}
			
					// unlock occupied resources
					// TODO: modularity, remove repeated code
					for (Map.Entry<String, Integer> entry : file_lock.entrySet()) {
						if (entry.getValue() == trans_ID) {
							file_lock.remove(entry.getKey());
						}

						try {
							// write log for file lock
							BufferedOutputStream writer = new
							BufferedOutputStream(new FileOutputStream(log_name, true));
							
							writer.write(("fileUnlock\n").getBytes());
							String log_line = Integer.toString(entry.getValue()) + ":" + 
																	entry.getKey() + "\n";
							writer.write(log_line.getBytes());
							writer.flush();
							writer.close();
							//PL.fsync();
							
						} catch (Exception e) {
							System.out.println( "I/O Error " + e.getMessage());
							e.printStackTrace();
						}
					}
				}
				else {
					System.out.println( myId + ": cancel decision re-received or prepare "
																	+ "message missed.");
				}
			}
			else {
				System.out.println("Error: unexpected message type in " + msg.addr);
			}
			// send back ack
			msg = new MyMessage("Server", "ack".getBytes(), trans_ID);
			System.out.println( myId + ": Sending back ack for " + mmsg.decision );
			PL.sendMessage(msg);
		}
		else {
			System.out.println("Error: unexpected message type in " + msg.addr);
		}

		return true;
	}
	
	/*
	 * recovery: When UserNode restart, check the log file and recover any lost status.
	 */
	public static synchronized void recovery() {
		// check if log file exist
		File f_log = new File(log_name);
		if (f_log.exists()) {
			System.out.println( myId + ": find log, start recovery. " );

			byte buffer[] = new byte[(int) f_log.length()];

			try {
				// read from the log
				BufferedInputStream reader = new 
				BufferedInputStream(new FileInputStream(log_name));

				reader.read(buffer);
				reader.close();
				
			} catch (Exception e) {
				System.out.println( "I/O Error " + e.getMessage());
				e.printStackTrace();
			}

			String[] parsed_log = new String(buffer).split("\n");
			String status, srcName;
			int trans_ID;
		
			// update trans status and file lock; read 2 lines one time
			for (int i = 0; i < parsed_log.length; i += 2) {
				if (parsed_log[0].equals("trans")) {
					trans_ID = Integer.parseInt(parsed_log[i + 1].split(":")[0]);
					status = parsed_log[i + 1].split(":")[1];
					if (status.equals("start")) {
						trans_status.put(trans_ID, false);
					}
					else if (status.equals("done")) {
						trans_status.put(trans_ID, true);
					}
					else if (status.equals("cancel")) {
						trans_status.remove(trans_ID);
					}
				}
				else {
					trans_ID = Integer.parseInt(parsed_log[i + 1].split(":")[0]);
					srcName = parsed_log[i + 1].split(":")[1];
					if (parsed_log[0].equals("fileLock")) {
						file_lock.put(srcName, trans_ID);
					}
					else if (parsed_log[0].equals("fileUnlock")) {
						file_lock.remove(srcName);
					}
				}
			}
		}
	}

	public static synchronized void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		System.out.println( args[1] + " initialized. ");
		
		recovery();
	}
}

