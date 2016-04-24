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
    public InetAddress IP;
    public Map<Integer, ProcessInfo> nodes;

    // For receiving acknowledgements.
    public int receiving_port;
    public ServerSocket receiving_socket;
    boolean need_ack;
    public ProcessInfo self_info;

    // Stores commands until they can be sent to the nodes.
    public Queue<String> Q;

    public Lock mutex;

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
            this.Q = new ConcurrentLinkedQueue<String>();
            this.need_ack = false;
            this.mutex = new ReentrantLock(true);
            this.nodes = new HashMap<Integer, ProcessInfo>();
            this.IP = InetAddress.getByName("127.0.0.1");
            this.self_info = new ProcessInfo(-1, client_port, this.IP);
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
        int retval = 0;

        String[] tokens = line.split(" ");

        // <command> <new_node_id> <node0 ip> <node0 port>
        if(tokens[0].equals("join") && (tokens.length >= 2))
        {		    	 
            int node_id = Integer.parseInt(tokens[1]);
            // Adding Node 0 information.
            String message =    "join"
                                + " " + node_id
                                + " " + "127.0.0.1"
                                + " " + Integer.toString(base_port);
            this.mutex.lock();
            if (!this.need_ack) {
                this.join(node_id, message);
            }
            else {
                this.Q.add(message);
                this.need_ack = true;
            }
            this.mutex.unlock();
        }
        else if (tokens[0].equals("show"))
        {
            if (tokens[1].equals("all"))
            {
                for (int i = 0; i < 256; i++)
                {
                    this.mutex.lock();
                    if (!this.need_ack) {
                        this.show(i, "N");
                    }
                    else {
                    // N means do not print an error message if node DNE.
                        this.Q.add("show " + Integer.toString(i) + " N");
                        this.need_ack = true;
                    }
                    this.mutex.unlock();
                }
            }
            else
            {
                int node_id = Integer.parseInt(tokens[1]);
                this.mutex.lock();
                if (!this.need_ack) {
                    this.show(node_id, "P");
                }
                else {
                    // P means print an error message if node DNE.
                    this.Q.add("show " + Integer.toString(node_id) + " P");
                    this.need_ack = true;
                }
                this.mutex.unlock();
            }
        }
        else
        {
            System.out.println("Command not found");
        }
        
        return retval;
    }

    public void show(int node_id, String print)
    {
        ProcessInfo node = this.nodes.get(node_id);
        if (node != null && node.alive)
        {
            Runnable sender = new ClientSender(node, "show " + Integer.toString(node_id));
            new Thread(sender).start();
            this.need_ack = true;
        }
        else
            if (print.equals("P"))
                System.out.println(Integer.toString(node_id) + " does not exist");
    }

    /*
     * Runs on its own thread to receive and handle acknowledgements from the
     * replica server.
     */
    public void receive_ack() throws IOException
    {
        Socket receiver = this.receiving_socket.accept();
        Runnable sender;
        String message;
        int node_id;

        // Receives message.
        DataInputStream in = new DataInputStream(receiver.getInputStream());
        String ack = "";
        ack = in.readUTF();

        // Handles the next message in Q since the ack from the previous was
        // received. If there is no message to be sent, then nothing happens
        // and need_ack remains false.
        this.mutex.lock();
        this.need_ack = false;
        try{
            if (!this.Q.isEmpty()) {
                message = this.Q.poll();
                String[] tokens = message.split(" ");
                String action = tokens[0];
                node_id = Integer.parseInt(tokens[1]);
                switch (action) {
                    case "join":
                        this.join(node_id);
                        break;
                    case "crash":
                        break;
                    case "find":
                        break;
                    case "show":
                        this.show(node_id, tokens[2]);
                        break;
                }
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.mutex.unlock();

        System.out.println(ack);

        receiver.close();
    }

    /*
     * Creates a new node, then sends it a join command.
     */
    public void join(int node_id, String message)
    {
        ProcessInfo exists = this.nodes.get(node_id);
        if (exists != null && exists.alive)
            System.out.println("Node " + Integer.toString(node_id) + " already exists in the system.");
        else
        {
            ProcessInfo new_info = new ProcessInfo(node_id, this.base_port + node_id, this.IP);
            this.nodes.put(node_id, new_info);
            try {
                ServerNode new_node = new ServerNode(new_info.Id, new_info.portNumber, this.self_info);
                new_node.start();
                sleep(1);
            } catch (IOException e){
                e.printStackTrace();
            } catch (InterruptedException e){
                e.printStackTrace();
            }
            Runnable sender = new ClientSender(new_info, message);
            new Thread(sender).start();
            this.need_ack = true;
         }
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
                    client.prepare_cmd(cmd);
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
            catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }
}
