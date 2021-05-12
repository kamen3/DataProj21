/** Mine  */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

class ServiceThread implements Runnable
{
    private Socket client;
    private int R;
    private int timeout;
    private int rebalance_period;
    private ConcurrentLinkedQueue<String[]> commandQueue;
    private ConcurrentHashMap<String, FileIndex> fileIndex;
    private ConcurrentHashMap<Integer, Socket> storeIndex;

    public ServiceThread(Socket client_, int R_, int timeout_, int rebalance_period_, ConcurrentLinkedQueue<String[]> commandQueue_,
                         ConcurrentHashMap<String, FileIndex> fileIndex_, ConcurrentHashMap<Integer, Socket> storeIndex_)
    {
        client = client_;
        R = R_;
        timeout = timeout_;
        rebalance_period = rebalance_period_;
        commandQueue = commandQueue_;
        fileIndex = fileIndex_;
        storeIndex = storeIndex_;
    }

    public void run()
    {
        try
        {
            BufferedReader bfin = new BufferedReader(new InputStreamReader(client.getInputStream()));
            PrintWriter prout = new PrintWriter(client.getOutputStream(), true);

            String inpLine;
            inpLine = bfin.readLine();

            String[] comArgs = inpLine.split(" ");
            String command = comArgs[0];

            /**
             * DStore joining
             */
            if(command.equals(Protocol.JOIN_TOKEN))
            {
                storeIndex.put(Integer.parseInt(comArgs[1]), client);
                /** Rebalance later */
            }

            /**
             * Hmmm... How to write the Client behaviour so that it doesn't depend on what
             * the first sent line is?
             * Somehow separate it in a different class? With the line passed into
             * the constructor?
             * And all the different behaviour separated into different methods?
             *
             *
             *
             * Or wait, what if the behaviour is split into different methods  HERE????
             * **/


            /** Otherwise it's a client asking for something
             *  And in here there will be the while loops on the streams
             *  cuz they're all just running in different threads.
             *
             *  And once the connection is closed from the client's side
             *  then it is closed on this, too, and the thread terminates.
             *  Beautiful.
             * */

            /**
             * Client asks for a list
             */
            else if(command.equals(Protocol.LIST_TOKEN))
            {
                PrintWriter out = new PrintWriter(client.getOutputStream(), true);

                if(storeIndex.size() < R)
                {
                    out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    client.close();
                }

                String message = "LIST";
                String[] files = fileIndex.keySet().toString().split(", ");

                for(String name: files)
                {
                    message += " " + name.substring(1, name.length()-2);
                }

                out.println(message);

                out.close();
                client.close();
            }

            else if(command.equals(Protocol.STORE_TOKEN))
            {

            }


            else System.out.println("unrecognised command");// Will probably have to call logger here


        }catch(Exception e){System.out.println("Actually something went wrong somewhere");}
    }
}