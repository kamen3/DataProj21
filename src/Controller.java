import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Controller
{
    public static void main(String[] args) throws IOException
    {
        try
        {
            final int cport = Integer.getInteger(args[0]);
            final int R = Integer.getInteger(args[1]);
            final int timeout = Integer.getInteger(args[2]);
            final int rebalance_period = Integer.getInteger(args[3]);

            ServerSocket ss = new ServerSocket(cport);
            ConcurrentLinkedQueue<String[]> commandQueue = new ConcurrentLinkedQueue<String[]>();

            // Will be used for getting info about files, based on their filename
            ConcurrentHashMap<String, FileIndex> fileIndex = new ConcurrentHashMap<String, FileIndex>();

            // Will be used for quick access to stores, based on their socket number
            ConcurrentHashMap<Integer, Socket> storeIndex = new ConcurrentHashMap<Integer, Socket>();

            // Will be used to keep an order of which DStores are least "full" - used for properly storing new files
            Vector<DStoreIndex> storeVector = new Vector<DStoreIndex>();

            for(;;)
            {
                try
                {
                    Socket client = ss.accept();

                    new Thread(new ServiceThread(client, R, timeout, rebalance_period, commandQueue, fileIndex, storeIndex, storeVector)).start();
                }
                catch(Exception e){System.out.println("error "+e);}
            }
        }
        catch(Exception e){System.out.println("error "+e);}
        System.out.println();
    }
}
