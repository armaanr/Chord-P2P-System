import java.net.*;
import java.util.*;
import java.io.*;

/*
 * Sends a message in a new thread to the client's replica server.
 */
public class ClientSender implements Runnable
{
    public String cmd;
    public ProcessInfo rec_node;

    /*
     * Creates a runnable class that makes a new thread
     * for each command that is sent.
     */
    public ClientSender(ProcessInfo rec_node, String cmd)
    {
        this.rec_node = rec_node;
        this.cmd = cmd;
    }

    /* 
     * Sends a cmd to the assigned replica server.
     * <command> <new node id> <node0 ip> <node0 port> <client port>
     */
    private void send_cmd() throws IOException 
    {
        InetAddress destIp = this.rec_node.IP;
        int destPort = this.rec_node.portNumber;
           
        Socket sendSock = new Socket(destIp, destPort);	   
        DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());

        out.writeUTF(this.cmd);
        
        sendSock.close();
    }

    /*
     * Runs a thread for each cmd that is sent to the replica.
     */
    public void run()
    {
        try {
            if (Thread.interrupted())
            {
                try {
                    throw new InterruptedException();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            send_cmd();
        }
        catch (IOException e) {
             e.printStackTrace();
        }
    }
}
