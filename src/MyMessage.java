/* MyMessage.java */

import java.io.*;
import java.util.ArrayList;

public class MyMessage extends ProjectLib.Message{
    public Integer trans_ID;
    public String addr;
    public byte[] body; // indicate the type of the message
    public ArrayList<String> srcNames;
    public byte[] img;
    public String[] sources;
    public String decision;
    public String filename;

    public MyMessage( String addr, byte[] body ) {
        super(addr, body);
    }

    /* For Server */
    // prepare type
    public MyMessage( String addr, byte[] body, int trans_ID, ArrayList<String> srcNames, 
                                            byte[] img, String[] sources ) {
        super(addr, body);
        this.trans_ID = trans_ID;
        this.srcNames = srcNames;
        this.img = img;
        this.sources = sources;
    }

    // decision type
    public MyMessage( String addr, byte[] body, int trans_ID, ArrayList<String> srcNames, 
                                            String decision, String filename ) {
        super(addr, body);
        this.trans_ID = trans_ID;
        this.srcNames = srcNames;
        this.decision = decision;
        this.filename = filename;
    }

    /* For UserNode */
    // reply type
}
