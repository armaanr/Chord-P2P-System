import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Client extends Thread
{
    // Information from configuration file.
    public int min_delay;
    public int max_delay;
    public int base_port;
    public Map<Integer, boolean> node_status;

    // For receiving acknowledgements.
    public int receiving_port;
    public ServerSocket receiving_socket;
    boolean need_ack;

    // For sending commands to the replica.
    public int server_id;
    public int replica_port;
    public InetAddress replica_ip;
    // Stores commands until they can be sent to the replica.
    public Queue<String> Q;

    public Lock mutex;

    // For handling timeouts.
    public long last_sent;

    /*
     * If this client's replica crashes, it must connect to the replica
     * with the next higher id. reset() takes care of this and resends
     * the last message sent to the crashed replica.
     */
    public void reset(String cmd)
    {
        System.out.println("Switching to server with next higher id.");
        Runnable sender;

        this.server_id = ((this.server_id+1) % this.otherProcesses.size());
        if (this.server_id == 0)
            this.server_id++;
        this.replica_port = this.otherProcesses.get(server_id).getPort();
        this.replica_ip = this.otherProcesses.get(server_id).getIP();
        // Send the command to the new replica if it should be resent.
        if (!cmd.equals("drop"))
        {
            sender = new ClientSender(this, cmd);
            new Thread(sender).start();
        }
    }

    /*
     * Client constructor fills in basic fields from command line.
     * The rest of the fields are filled in by the configuration
     * file parser.
     */
    public Client(int client_port)
    {
        try
        {
            this.receiving_port = client_port;
            this.receiving_socket = new ServerSocket(client_port, 10);
            this.receiving_socket.setSoTimeout(1000000);
            // Initializes all nodes as nonexistent.
            this.node_status = new HashMap<Integer, boolean>();
            for (int i = 0; i < 256; i++) {
                node_status.put(i, false);
            }
            this.Q = new ConcurrentLinkedQueue<String>();
            this.need_ack = false;
            this.mutex = new ReentrantLock(true);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /* 
     * Parses commands and executes the applicable function.
     * Formats the command based on the protocol given in the MP specs
     * before sending it to the assigned replica.
     */
    public int prepare_cmd(String line) throws IOException
    {
        @SuppressWarnings("resource")
        String cmd;
        int retval = 0;
        Runnable sender;

        String[] tokens = line.split(" ");

        // <new_node_id> <node0 ip> <node0 port>
        if(tokens[0].equals("join") && (tokens.length >= 2))
        {		    	 
            int node_id = Integer.parseInt(tokens[1]);
            String start_message = node_id
                                    + " " + "127.0.0.1"
                                    + " " + Integer.toString(base_port);
            this.mutex.lock();
            if (!this.need_ack) {
                ServerNode new_node = new ServerNode(node_id, this.base_port + node_id);
                new_node.start();
                sender = new ClientSender(this, message);
                new Thread(sender).start();
            }
            else {
                this.Q.add(message);
                this.need_ack = true;
            }
            this.mutex.unlock();
        }
        else
        {
            System.out.println("Command not found");
        }
        
        return retval;
    }

    /* 
     * Parses the given config file to initialize the client variables.
     */
    public void readConfig(File file) throws IOException
    {
        @SuppressWarnings("resource")
        Scanner scanner = new Scanner(file);

        if(scanner.hasNext())
        {
            String[] delays = scanner.nextLine().split(" ");
            this.min_delay = Integer.parseInt(delays[0]);
            this.max_delay = Integer.parseInt(delays[1]);

            String[] tokens = scanner.nextLine().split(" ");
            this.base_port = Integer.parseInt(tokens[0]);
        }
    }

    public static void main(String [] args) throws IOException
    {
        InputStream is = null;
        int i;
        char c;
        String cmd = "";
    
        // java Client <client port> <config filename>  
        try
        {
            // Gets client's id and server's id from command line args.
            int client_port = Integer.parseInt(args[0]);
            Client client = new Client(client_port);

            // Parses config file get information about node 0.
            String fileName = args[1];
            File file = new File(fileName);
            client.readConfig(file);

            // Start listening for ack's.
            client.start();

            // Creates Node 0.
            client.prepare_cmd("join 0");

            System.out.println("Enter commands:");

            // Reads until the end of the stream or user enters "exit"
            while((i = System.in.read()) != -1)
            {
                c = (char) i;

                // Send the command if user presses presses enter.
                if (c == '\n')
                {
                    should_exit = client.prepare_cmd(cmd);
                    cmd = "";
                }
                else
                {
                    // Adds read char to cmd string.
                    cmd += c;
                }
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        finally
        {
            // Releases system resources associated with this stream.
            if(System.in != null)
                System.in.close();
        }
    }

    /*
     * Runs on its own thread to receive and handle acknowledgements from the
     * replica server.
     */
    public void receive_ack() throws IOException
    {
        Socket receiver = this.receiving_socket.accept();
        Runnable sender;
        String cmd;

        // Receives message.
        DataInputStream in = new DataInputStream(receiver.getInputStream());
        String ack = "";
        ack = in.readUTF();

        // Sends the next message in Q since the ack from the previous was
        // received. If there is no message to be sent, then nothing happens
        // and need_ack remains false.
        this.mutex.lock();
        this.need_ack = false;
        if (!this.Q.isEmpty()) {
            cmd = this.Q.poll();
            sender = new ClientSender(this, cmd);
            new Thread(sender).start();
            this.need_ack = true;
        }
        this.mutex.unlock();

        System.out.println(ack);

        receiver.close();
    }

    public void run()
    {
        while (true) {
            try {
                if (Thread.interrupted())
                {
                    try {
                        throw new InterruptedException();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                receive_ack();
            }
            catch (SocketTimeoutException s) {
                if (this.scheme.equals("E") && this.need_ack && (System.currentTimeMillis() - this.max_delay*2) > this.last_sent)
                {
                    this.need_ack = false;
                    this.reset("drop");
                    System.out.println("No ack received");
                }
//                break;
            }
            catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }
}
