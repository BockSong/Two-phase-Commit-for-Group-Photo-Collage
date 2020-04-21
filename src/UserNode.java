import java.io.File;
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
	public final String myId;
	public static ProjectLib PL;
	// map all existing transaction ID with their status (if finished)
	private static ConcurrentHashMap<Integer, Boolean> trans_status = new 
							ConcurrentHashMap<Integer, Boolean>();
	// store the <trans_ID, file> pairs for exclusive acces to occupied files
	private static ConcurrentHashMap<Integer, File> file_lock = new 
							ConcurrentHashMap<Integer, File>();

	// lock for read and then modify file_lock
	private static Object rw_lock = new Object();
	
	public UserNode( String id ) {
		myId = id;
	}

	/*
	 * get_reply: get the reponse for a prepare message from the server.
	 */
	private static String get_reply(int trans_ID, File pic, byte[] img, String[] sources) {
		// if the image is occupied, return notok
		synchronized (rw_lock) {
			if (file_lock.containsValue(pic)) {
				return "notok";
			}
			// lock the involved image
			file_lock.put(trans_ID, pic);
		}

		// if the image is still there and user agree with that
		if (pic.exists() && PL.askUser(img, sources))
			return "ok";
		// if not, return notok
		else
			return "notok";
	}

	/*
	 * deliverMessage: Callback called when message received.
	 * Return: True if the message is handled, or False if not.
	 */
	public synchronized boolean deliverMessage( ProjectLib.Message msg ) {
		String msg_body = new String(msg.body);
		System.out.println( myId + ": Got message " + msg_body + " from " + msg.addr );

		MyMessage mmsg = (MyMessage) msg;
		int trans_ID = mmsg.trans_ID;
		String srcName = mmsg.srcName;
		File pic = new File("." + myId + "/" + srcName);

		// if it's a prepare message
		if (msg_body.equals("prepare")) {
			byte[] img = mmsg.img;
			String[] sources = mmsg.sources;

			// init a transaction
			trans_status.put(trans_ID, false);

			String reply = get_reply(trans_ID, pic, img, sources);

			// send the reply
			mmsg = new MyMessage("Server", reply.getBytes());
			System.out.println( myId + ": Sending reply of \"" + reply + "\"to Server." );
			PL.sendMessage(mmsg);
		}
		// if it's a decision
		else if (msg_body.equals("decision")) {
			// decision of done
			if (msg_body.equals("done")) {
				// delete sources images from the UserNode directories
				if (!pic.delete()) {
					System.out.println("Error: delete file failed from " + myId + "/" + srcName);
				}
	
				// mark the transaction as finished
				if (trans_status.containsKey(trans_ID)) {
					trans_status.put(trans_ID, true);
				}
				else {
					System.out.println( "Error: cannot find this transaction.");
				}
	
				// unlock occupied resources
				file_lock.remove(trans_ID);
			}
			// decision of cancel
			else if (msg_body.equals("cancel")) {
				// TODO: should we do it here, or earlier once askUser is false?
				// cancel the transaction
				if (trans_status.containsKey(trans_ID)) {
					trans_status.remove(trans_ID);
				}
				else {
					System.out.println( "Error: cannot find this transaction.");
				}

				// unlock occupied resources
				file_lock.remove(trans_ID);
			}
			else {
				System.out.println("Error: unexpected message type from Server to " + mmsg.addr);
			}
			// send back ack
			msg = new ProjectLib.Message("Server", "ack".getBytes());
			System.out.println( myId + ": Sending ack to Server " );
			PL.sendMessage(msg);
		}
		else {
			System.out.println("Error: unexpected message type from Server to " + mmsg.addr);
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

