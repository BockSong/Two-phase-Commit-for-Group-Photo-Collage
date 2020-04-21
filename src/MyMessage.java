/* MyMessage.java */

import java.io.*;

public class MyMessage extends ProjectLib.Message{
    public Integer trans_ID;
    public String addr;
    public byte[] body; // indicate the type of the message
    public String srcName;
    public byte[] img;
    public String[] sources;
    public String decision;
    public String filename;

    public MyMessage( String addr, byte[] body ) {
        super(addr, body);
    }

    /* For Server */
    // prepare type
    public MyMessage( String addr, byte[] body, int trans_ID, String srcName, 
                                            byte[] img, String[] sources ) {
        super(addr, body);
        this.trans_ID = trans_ID;
        this.srcName = srcName;
        this.img = img;
        this.sources = sources;
    }

    // decision type
    public MyMessage( String addr, byte[] body, int trans_ID, String srcName, 
                                            String decision, String filename ) {
        super(addr, body);
        this.trans_ID = trans_ID;
        this.srcName = srcName;
        this.decision = decision;
        this.filename = filename;
    }

    /* For UserNode */
    // reply type
}
