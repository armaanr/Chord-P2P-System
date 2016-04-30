import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.*;
import java.io.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.omg.CORBA.portable.InputStream;

public class ServerNode extends Thread {
    String join_report_file = "join_report.txt";
    String find_report_file = "find_report.txt";

    // Due to concurrency issues.
    int ack_count;
    public Lock mutex;

	int Id;
	ProcessInfo pred;
	ProcessInfo[] fingerTable;
    IntervalInfo[] ft_info;
    boolean[] keys;
    boolean[] duplicates;
    ProcessInfo self_info;
    ProcessInfo client;
    ProcessInfo node0;
    int min_delay;
    int max_delay;
    InetAddress localhost = InetAddress.getByName("127.0.0.1");

    boolean failStarter;
    Thread failChecker;
    boolean crash;
    
    ServerSocket server;
    
    public ServerNode(int Id, int port, ProcessInfo client) throws IOException
    {
        this.ack_count = 0;
        this.mutex = new ReentrantLock(true);

        this.Id = Id;
        this.server = new ServerSocket(port, 10, localhost);
        this.pred = null;
        this.fingerTable = new ProcessInfo[8];
        // Stores useful interval information for easy comparison.
        this.ft_info = new IntervalInfo[8];
        this.failStarter = false;
        this.failChecker = null;
        this.crash = false;
        // Initialized to false.
        this.keys = new boolean[256];
        this.duplicates = new boolean[256];
        this.self_info = new ProcessInfo(Id, port, localhost);
        this.client = client;
        for (int i = 0; i < 8; i++)
        {
            this.ft_info[i] = new IntervalInfo(Id, i);
            // Just initializing the values, doesn't matter what they are.
            this.fingerTable[i] = this.self_info;
        }
    }
    
    public void ping()
    {
        System.out.println("Starting pinger for " +this.Id+ "with new pred => "+this.pred.Id);
        this.failChecker = new Pinger(this.pred, this.self_info);
        this.failChecker.start();
        
    }
    
    private void failDetectHandler() {
        System.out.println("entered failure detecter in "+ this.Id);
        int crashedId = pred.Id;
        this.failStarter = true;
        System.out.println("Crashed => " + crashedId);
        
        int newPredecessorId = newPredFinder(crashedId);
        System.out.println("switched pre to "+ newPredecessorId);
        for(int i = newPredecessorId+1 ; i < (crashedId+1)%256; i++)
        {
            this.keys[i] = true;
        }
        
        this.pred = new ProcessInfo(newPredecessorId, this.node0.portNumber + newPredecessorId, localhost);
        
        this.failChecker.interrupt();
        ping();
        
        String message = "fail " + crashedId + " " + this.Id + " " + this.pred.Id ;
        Runnable send = new ClientSender(this.pred, message);
        new Thread(send).start();
        
        String succMessage = "dupsUpdate "+ this.pred.Id + " "+ crashedId;
        Runnable send2 = new ClientSender(this.fingerTable[0], succMessage);
        new Thread(send2).start();
    }
    
    public void dupsUpdate(int start, int end )
    {
        System.out.println("updating dups between " + start + " and "+end);
        
        for(int i = start+1 ; i < (end+1)%256; i++)
        {
            this.duplicates[i] = true;
        }
    }
    
    private int newPredFinder(int crashedId) {
        int newPredecessorId = -1;
        
        for(int i = crashedId ; i>=0; i--)
        {
            if(this.duplicates[i] == false)
            {
                System.out.println("duplicate looked at = " + i);
                newPredecessorId = i;
                break;
            }
        }
        
        return newPredecessorId;
    }
    
    
    public void crashUpdater(int crashId, int replace, int crashPred)
    {
        System.out.println("entered crash updater in "+ this.Id);
        
        if(this.failStarter || ( (this.Id+128)%256 <= crashPred ) )
        {
            System.out.println("reached failStarter");
            String message = "crash" + crashId;
            ack_sender(message);
            return;
        }
        
        for(int i = 0; i< fingerTable.length ; i++)
        {
            if(fingerTable[i].Id == crashId)
            {
                fingerTable[i] = new ProcessInfo(replace, this.node0.portNumber + replace, localhost);
            }
        }
        
        try {
            Socket sock = new Socket(this.pred.IP, this.pred.portNumber);
            String message = "fail " + crashId + " "+ replace + " " + crashPred;
            DataOutputStream failSend = new DataOutputStream(sock.getOutputStream());
            failSend.writeUTF(message);
            sock.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    
    public void nodeJoin(int node_id, String[] tokens)
    {
        // Set min/max delay values.
        this.min_delay = Integer.parseInt(tokens[4]);
        this.max_delay = Integer.parseInt(tokens[5]);
        // All nodes must be able to contact Node 0.
        this.node0 = new ProcessInfo(0,
                                     Integer.parseInt(tokens[3]),
                                     localhost);
        // Special case to initialize Node 0.
        if (node_id == 0)
        {
            for (int i = 0; i < 8; i++)
                this.fingerTable[i] = this.self_info;
            // All keys stored at Node 0 initially.
            for (int j = 0; j < 256; j++)
            {
                this.keys[j] = true;
                this.duplicates[j] = true;
            }
            this.pred = this.self_info;
            ping();
            this.ack_sender("A");
        }
        else
        {
            this.init_ft(node_id, this.node0);
        }
    }
    
    /*
     * Sends request for predecessor, successor, and successor finger table
     * to node0.
     */
    public void init_ft(int node_id, ProcessInfo node0)
    {
        String message = "j " + Integer.toString(node_id);
        this.delayGenerator();
        Runnable sender = new ClientSender(node0, message);
        new Thread(sender).start();
//        log(message);
    }

    /*
     * Can be used to log interactions between nodes. Messages used for the
     * find command are recorded in find_report_file and messages used for
     * join are recorded in join_report_file. This is the function used to
     * calculate the average number of messages in the report.
     */
    public void log(String message)
    {
        String[] tokens = message.split(" ");
        String report_file = "";
        if (tokens[0].equals("find") || tokens[0].equals("found"))
            report_file = this.find_report_file;
        else
            report_file = this.join_report_file;

        try{
            Writer output = new BufferedWriter(new FileWriter(report_file, true));
            output.append(message+"\n");
            output.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

    }
    
    /*
     * Checks if it is the successor of the new node. If it is not,
     * it will pass on the message to the node in its finger table
     * that is closest to and precedes the new node.
     */
    public void find_successor(int node_id, String purpose)
    {
        String[] tokens = purpose.split(" ");
        String message = "";

        // Checks if current node is node_id's successor.
        int pred_comp = this.pred.Id + 1;
        if (contains(node_id, this.pred.Id+1, this.Id+1))
        {
            switch(tokens[0]) {
                case "start":
                    this.update_as_successor(node_id);
                    break;
                case "i":
                    message = "u " + Integer.toString(this.Id) + " " + tokens[2];
                    int requester_id = Integer.parseInt(tokens[3]);
                    ProcessInfo receiver = new ProcessInfo(requester_id,
                                                            this.node0.portNumber+requester_id,
                                                            localhost);
                    this.delayGenerator();
                    Runnable sender = new ClientSender(receiver, message);
                    new Thread(sender).start();
//                    this.log(message);
                    break;
            }
        }
        // If this node is not the successor of node_id, then forward the
        // request to the closest preceding finger of node_id based on this
        // node's finger table.
        else {
            ProcessInfo cpf = this.closest_preceding_finger(node_id);
            switch(tokens[0]) {
                case "start":
                    message = "j " + Integer.toString(node_id);
                    break;
                case "i":
                    message = purpose;
                    break;
            }
            this.delayGenerator();
            Runnable sender = new ClientSender(cpf, message);
            new Thread(sender).start();
//            this.log(message);
        }
    }
    
    /*
     * Updates the predecessors and initiates the updating of all other nodes whose
     * finger tables might have changed as a result of the new node joining the system.
     * Similar to the function "update_others" in the MIT paper.
     */
    public void update_as_pred(int node_id, int pred_id)
    {
        ProcessInfo new_node = new ProcessInfo(node_id,
                                               this.node0.portNumber+node_id,
                                               localhost);
        
        for (int i = 0; i < 8; i++)
        {
            if (contains(node_id, this.ft_info[i].start, this.fingerTable[i].Id))
            {
                this.fingerTable[i] = new_node;
            }
        }
        
        String message;
        ProcessInfo receiver;
        int termination_id = ((pred_id - 127) + 256) % 256;
        if (contains(termination_id, this.pred.Id+1, this.Id+1)) {
            // The the 0 is place holder for tokens[1] in the receiving thread.
            // It can be replaced with a useful number if necessary.
            message = "finished 0";
            receiver = new ProcessInfo(node_id,
                                       this.node0.portNumber+node_id,
                                       localhost);
        }
        else {
            message = "p"
            + " " + Integer.toString(node_id)
            + " " + Integer.toString(pred_id);
            receiver = this.pred;
        }
        this.delayGenerator();
        Runnable sender = new ClientSender(receiver, message);
        new Thread(sender).start();
//        this.log(message);
    }
    
    /*
     * Called when a node realizes that it is the successor of the
     * new node being added to the network. This node's predecessor
     * and keys will be updated. This node's predecessor, finger table,
     * and duplicate keys will be sent to the new node.
     */
    public void update_as_successor(int node_id)
    {
        String duplicates = "";
        // The successor's current duplicates will become the new node's duplicates.
        for (int i = 0; i < 256; i++)
        {
            if (this.duplicates[i])
            {
                duplicates += " " + Integer.toString(i);
                this.duplicates[i] = false;
            }
        }
        // The successor will lose some keys to the new node. The keys that it
        // loses will now become its duplicates.
        for (int j = this.pred.Id+1; j % 256 != (node_id + 1) % 256 ; j++)
        {
            this.keys[j % 256] = false;
            this.duplicates[j % 256] = true;
        }

        // The successor's final keys become the super successor's duplicates.
        String super_successor_duplicates = "";
        for (int k = 0; k < 256; k++)
            if (this.keys[k])
                super_successor_duplicates += " " + Integer.toString(k);

        // Need to update super successor's duplicates as well.
        if (!(this.Id == 0 && this.pred.Id == 0))
        {
            String update_super_message = "dups"+ super_successor_duplicates;
            this.delayGenerator();
            Runnable update_super = new ClientSender(this.fingerTable[0], update_super_message);
            new Thread(update_super).start();
    //        this.log(super_successor_duplicates);
         }
        
        // Notify the current predecessor to update its successor to the newly joined node.
        int old_pred_id = this.pred.Id;
        String update_message = "p"
                                + " " + Integer.toString(node_id)
                                + " " + Integer.toString(this.pred.Id);
        this.mutex.lock();
        this.ack_count++;
        this.mutex.unlock();

        this.delayGenerator();
        Runnable update_sender = new ClientSender(this.pred, update_message);
        new Thread(update_sender).start();
//        this.log(update_message);

        // Update predecessor to the newly joined node.
        this.pred = new ProcessInfo(node_id,
                                    this.node0.portNumber+node_id,
                                    localhost);
        
        this.failChecker.interrupt();
        ping();
        
        if (old_pred_id == this.Id)
            this.fingerTable[0] = this.pred;

        // Gives new node its predecessor's id, successor's id,
        // successor's finger table, and successor's original duplicates.
        String message = "successor"
                        + " " + Integer.toString(old_pred_id)
                        + " " + Integer.toString(this.Id)
                        + " " + Integer.toString(this.fingerTable[0].Id)
                        + "," + Integer.toString(this.fingerTable[1].Id)
                        + "," + Integer.toString(this.fingerTable[2].Id)
                        + "," + Integer.toString(this.fingerTable[3].Id)
                        + "," + Integer.toString(this.fingerTable[4].Id)
                        + "," + Integer.toString(this.fingerTable[5].Id)
                        + "," + Integer.toString(this.fingerTable[6].Id)
                        + "," + Integer.toString(this.fingerTable[7].Id)
                        + duplicates;
        this.delayGenerator();
        Runnable sender = new ClientSender(this.pred, message);
        new Thread(sender).start();
//        this.log(message);
    }

    /*
     * Find the closest node that is less than node_id.
     */
    public ProcessInfo closest_preceding_finger(int node_id)
    {
        for (int i = 7; i >= 0; i--)
            if (contains(this.fingerTable[i].Id, this.Id+1, node_id))
                return this.fingerTable[i];
        return this.fingerTable[0];
    }
    
    /*
     * Upon receiving a message from its successor, the new node will
     * set its predecessor and initialize its finger table. It will
     * also update its keys as necessary.
     */
    public void update_ft(int node_id, String[] tokens)
    {
        // Set the successor
        int succ_id = Integer.parseInt(tokens[2]);
        this.fingerTable[0] = new ProcessInfo(succ_id,
                                              this.node0.portNumber+succ_id,
                                              localhost);
        // Set the predecessor
        int pred_id = Integer.parseInt(tokens[1]);
        this.pred = new ProcessInfo(pred_id,
                                    this.node0.portNumber+pred_id,
                                    localhost);
        // Initialize the finger table.
        // Successor's finger table is provided in tokens[3], could be useful for
        // optimizations later on.
        for (int i = 0; i < 7; i++)
        {
            if (contains(this.ft_info[i+1].start, this.Id, this.fingerTable[i].Id+1))
            {
                this.fingerTable[i+1] = this.fingerTable[i];
            }
            else
            {
                // "i <start> <ft index> <node_id>"
                // start is the thing we want the successor for.
                // ft index is the index in which we will store our returned value.
                // node_id is so that the successor knows where to send its id.
                this.mutex.lock();
                this.ack_count++;
                this.mutex.unlock();
                String message = "i"
                                + " " + this.ft_info[i+1].start
                                + " " + Integer.toString(i+1)
                                + " " + Integer.toString(this.Id);
                this.delayGenerator();
                Runnable sender = new ClientSender(node0, message);
                new Thread(sender).start();
//                this.log(message);
            }
        }
        // Update the keys.
        for (int j = pred_id+1; j % 256 != (this.Id + 1) % 256 ; j++)
            this.keys[j % 256] = true;
        for (int l = 0; l < 256; l++)
            this.duplicates[l] = false;
        for (int k = 4; k < tokens.length; k++)
            if (!this.keys[Integer.parseInt(tokens[k])])
                this.duplicates[Integer.parseInt(tokens[k])] = true;
    }
    
    /*
     * Updates a finger table index to point to the info of the successor of ft[i].start
     */
    public void update_ft_entry(String[] tokens)
    {
        int index = Integer.parseInt(tokens[2]);
        int succ_id = Integer.parseInt(tokens[1]);
        this.fingerTable[index] = new ProcessInfo(succ_id,
                                                  this.node0.portNumber+succ_id,
                                                  localhost);
    }
    
    /*
     * Responds to client with node_id, finger table entries, and keys as an ACK.
     * The client cant directly print this message out since nodeShow takes care of
     * the formatting.
     */
    public void nodeShow(int node_id, String[] tokens)
    {
        String keys = "";
        String duplicates = "";
        for (int i = 0; i < 256; i++)
            if (this.keys[i])
                keys += " " + Integer.toString(i);
        for (int i = 0; i < 256; i++)
            if (this.duplicates[i])
                duplicates += " " + Integer.toString(i);
        String response = Integer.toString(node_id)
                          + "\n" + "FingerTable: " + this.fingerTable[0].Id
                          + "," + this.fingerTable[1].Id
                          + "," + this.fingerTable[2].Id
                          + "," + this.fingerTable[3].Id
                          + "," + this.fingerTable[4].Id
                          + "," + this.fingerTable[5].Id
                          + "," + this.fingerTable[6].Id
                          + "," + this.fingerTable[7].Id
                          + "\n" + "Keys:" + keys + "\n"
                          + "Duplicates:" + duplicates + "\n";
//                          + "pred: " + Integer.toString(this.pred.Id) + "\n";
        this.ack_sender(response);
    }

    /*
     * Checks if it has key. If not, forwards the request to the closest preceding
     * node to key in its finger table. If it does, it sends a "found" response to
     * the node that initiated the request so that it can send an ack to the client.
     */ 
    public void find(int node_id, int key)
    {
        String message;
        ProcessInfo nextNode = null;
        if(keys[key%256] == true)
        {
            message = "found " + Integer.toString(this.Id);
            nextNode = new ProcessInfo(node_id,
                                       this.node0.portNumber+node_id,
                                       localhost);
        }
        else
        {
            nextNode = this.closest_preceding_finger(key);
            message = "find " + node_id + " " + key+ " " + client.portNumber ;
        }
        this.delayGenerator();
        Runnable sender = new ClientSender(nextNode, message);
        new Thread(sender).start();
//        this.log(message);
    }

    //handles received messages
    private void receiver() throws IOException, ClassNotFoundException {
        
        if(this.failChecker != null)
        {
            
        }
        
        Socket receiver = server.accept();
        
        DataInputStream input = new DataInputStream(receiver.getInputStream());
        String message = "";
        message = input.readUTF();
        //        System.out.println("Server " + Integer.toString(this.Id) + " received: " + message);
        String[] tokens = message.split(" ");
        String action = tokens[0];
        int node_id = Integer.parseInt(tokens[1]);
        switch (action) {
            case "join":
                this.nodeJoin(node_id, tokens);
                break;
            case "dupsUpdate":
                int start = Integer.parseInt(tokens[1]);
                int end = Integer.parseInt(tokens[2]);
                this.dupsUpdate(start, end);
                break;
            case "predFailed":
                this.failDetectHandler();
                break;
            case "fail":
                int crashId = Integer.parseInt(tokens[1]);
                int replace = Integer.parseInt(tokens[2]);
                int crashPred = Integer.parseInt(tokens[3]);
                this.crashUpdater(crashId,replace, crashPred);
                break;
            case "crash":
                this.crash = true;
                this.failChecker.interrupt();
                break;
            case "find":
                int key = Integer.parseInt(tokens[2]);
                client = new ProcessInfo(-1, Integer.parseInt(tokens[3]) , localhost);
                this.find(node_id, key);
                break;
            case "show":
                nodeShow(node_id, tokens);
                break;
            case "j":
                this.find_successor(node_id, "start");
                break;
            case "i":
                this.find_successor(node_id, message);
                break;
            case "u":
                this.update_ft_entry(tokens);
                this.mutex.lock();
                this.ack_count--;
                // All updates are complete.
                if (this.ack_count == -1)
                {
//                    log("ACK sent!");
                    ping();
                    this.ack_sender("A");
                }
                this.mutex.unlock();
                break;
            case "successor":
                this.update_ft(node_id, tokens);
                break;
            case "p":
                this.update_as_pred(node_id, Integer.parseInt(tokens[2]));
                break;
            case "finished":
                this.mutex.lock();
                this.ack_count --;
                // All updates are complete.
                if (this.ack_count == -1)
                {
//                    log("ACK sent!");
                    ping();
                    this.ack_sender("A");
                }
                this.mutex.unlock();
                break;
            case "found":
                ack_sender(tokens[1]);
                break;
            case "dups":
                this.update_duplicates(tokens);
                break;
        }
        
        receiver.close();
        
    }

    /*
     * Updates duplicates to values in the given array.
     */
    public void update_duplicates(String[] duplicates)
    {
        for (int i = 0; i < 256; i++)
            this.duplicates[i] = false;
        for (int k = 1; k < duplicates.length; k++)
            this.duplicates[Integer.parseInt(duplicates[k])] = true;
    }
    
    /*
     * Sends an acknowledgement to the client.
     */
    public void ack_sender(String ack)
    {
        try {
            Socket sendSock = new Socket(this.client.IP, this.client.portNumber);
            DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
            out.writeUTF(ack);
            sendSock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void run()
	   {
           while(true)
           {
               try
               {
                   if(this.crash)
                   {
                       System.out.println("ending " + this.Id );
                       this.server.close();
                       break;
                   }
                   
                   receiver();
                   
                   
                   
                   
               }catch(SocketTimeoutException s)
               {
                   System.out.println("Socket timed out!");
                   break;
               }catch(IOException e)
               {
                   e.printStackTrace();
                   break;
               } catch (ClassNotFoundException e) {
                   // TODO Auto-generated catch block
                   e.printStackTrace();
               }
           }
       }
	
    /*
     * Can be used for testing. 
     */
	public static void main(String args[]) throws InterruptedException
	{
	}

    /*
     * Returns whether or not a given id falls
     * within this interval [start,end).
     */
    public boolean contains(int id, int start, int end)
    {
        return   (start < end && id >= start && id < end)
        ||(start > end && (id >= start || id < end))
        ||(start == end && start == 1 && this.pred.Id == 0 && this.fingerTable[0].Id == 0)
        ||(start == id);
    }
    
    /*
     * Computes the size of an interval. Useful in loops since
     * it is possible that the start > end.
     */
    public static int compute_size(int start, int end)
    {
        if (start > end)
            return 256 - start + end;
        else
            return start - end;
    }
    
    //generates delays based on min/max delay
    private void delayGenerator() {
        if(this.min_delay > 0 && this.max_delay > 0)
        {
            if(this.max_delay >= this.min_delay )
            {
                Random r = new Random();
                int randomDelay = r.nextInt(this.max_delay - this.min_delay) + this.min_delay;
                try {
                    Thread.sleep(randomDelay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else
                System.out.println("max is smaller than min");
        }
    }
}

