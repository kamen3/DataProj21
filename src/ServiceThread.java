/** Mine  */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Vector;
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
    Vector<DStoreIndex> storeVector;

    BufferedReader bfin;
    PrintWriter prout;

    String inpLine;
    String[] comArgs;
    String command;

    public ServiceThread(Socket client_, int R_, int timeout_, int rebalance_period_, ConcurrentLinkedQueue<String[]> commandQueue_,
                         ConcurrentHashMap<String, FileIndex> fileIndex_, ConcurrentHashMap<Integer, Socket> storeIndex_,
                         Vector<DStoreIndex> storeVector_)
    {
        client = client_;
        R = R_;
        timeout = timeout_;
        rebalance_period = rebalance_period_;
        commandQueue = commandQueue_;
        fileIndex = fileIndex_;
        storeIndex = storeIndex_;
        storeVector = storeVector_;
    }

    public void run()
    {
        try
        {
            bfin = new BufferedReader(new InputStreamReader(client.getInputStream()));
            prout = new PrintWriter(client.getOutputStream(), true);

            while ((inpLine = bfin.readLine()) != null)
            {
                comArgs = inpLine.split(" ");
                command = comArgs[0];

                if(command.equals(Protocol.JOIN_TOKEN)) actOnJoin();
                else if(command.equals(Protocol.LIST_TOKEN)) actOnList();
                else System.out.println("unrecognised command");// Will probably have to call logger here
            }

            bfin.close();
            prout.close();
            client.close();
        }
        catch(Exception e) {System.out.println("uh oh stinkyyyyy");}
    }

    private void actOnJoin()
    {
        storeIndex.put(Integer.parseInt(comArgs[1]), client);
        /** Rebalance later */
    }

    private void actOnList()
    {
        try
        {
            if (storeIndex.size() < R)
            {
                prout.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                client.close();
            }

            String message = "LIST";
            String[] files = fileIndex.keySet().toString().split(", ");

            for (String name : files)
            {
                message += " " + name.substring(1, name.length() - 2);
            }

            prout.println(message);
        }
        catch(Exception e){System.out.println("uh oh stinky2");}
    }
}