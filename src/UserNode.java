import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

public class UserNode implements ProjectLib.MessageHandling {
	public static String myId;
	public static ProjectLib PL;
	// map all existing transaction ID with their status (if finished)
	private static ConcurrentHashMap<Integer, Boolean> trans_status = new 
							ConcurrentHashMap<Integer, Boolean>();
	// store <file, trans_ID> pairs for exclusive access to every occupied files
	private static ConcurrentHashMap<File, Integer> file_lock = new 
							ConcurrentHashMap<File, Integer>();

	// lock for read and then modify file_lock
	private static Object rw_lock = new Object();
	
	public UserNode( String id ) {
		myId = id;
	}

	/*
	 * get_reply: get the reponse for a prepare message from the server.
	 */
	private synchronized static String get_reply(MyMessage mmsg) {
		for (String srcName: mmsg.srcNames) {
			File pic = new File(srcName);

			// check if the image is still there
			if ( !pic.exists() ) {
				return "notok";
			}

			synchronized (rw_lock) {
				// if the image is occupied, return notok
				if (file_lock.containsKey(pic)) {
					return "notok";
				}
				// lock the involved image
				file_lock.put(pic, mmsg.trans_ID);
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

			String reply = get_reply(mmsg);

			// send the reply
			mmsg = new MyMessage("Server", "reply2ask".getBytes(), reply);
			System.out.println( myId + ": Sending reply of \"" + reply + "\"." );
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
						System.out.println("Error: delete file failed from " + myId + "/" + srcName);
					}
					else {
						System.out.println( myId + ": " + srcName + " deleted successfully. " );
					}
				}

				// mark the transaction as finished
				if (trans_status.containsKey(trans_ID)) {
					trans_status.put(trans_ID, true);
				}
				else {
					System.out.println( "Error: cannot find this transaction.");
				}
	
				// unlock occupied resources
				for (Map.Entry<File, Integer> entry : file_lock.entrySet()) {
					if (entry.getValue() == trans_ID) {
						file_lock.remove(entry.getKey());
					}
				}
			}
			// decision of cancel
			else if (mmsg.decision.equals("cancel")) {
				// TODO: should we do it here, or earlier once askUser is false?
				// cancel the transaction
				if (trans_status.containsKey(trans_ID)) {
					trans_status.remove(trans_ID);
				}
				else {
					System.out.println( "Error: cannot find this transaction.");
				}

				// unlock occupied resources
				for (Map.Entry<File, Integer> entry : file_lock.entrySet()) {
					if (entry.getValue() == trans_ID) {
						file_lock.remove(entry.getKey());
					}
				}
			}
			else {
				System.out.println("Error: unexpected message type in " + msg.addr);
			}
			// send back ack
			msg = new ProjectLib.Message("Server", "ack".getBytes());
			System.out.println( myId + ": Sending back ack for " + mmsg.decision );
			PL.sendMessage(msg);
		}
		else {
			System.out.println("Error: unexpected message type in " + msg.addr);
		}

		return true;
	}
	
	public static synchronized void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		System.out.println( args[1] + " initialized. ");
		
		//ProjectLib.Message msg = new ProjectLib.Message( "Server", "hello".getBytes() );
		//System.out.println( args[1] + ": Sending message to " + msg.addr );
		//PL.sendMessage( msg );
	}
}

