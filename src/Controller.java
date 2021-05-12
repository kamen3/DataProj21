import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
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
            ConcurrentHashMap<String, FileIndex> fileIndex = new ConcurrentHashMap<String, FileIndex>();
            ConcurrentHashMap<Integer, Socket> storeIndex = new ConcurrentHashMap<Integer, Socket>();

            for(;;)
            {
                try
                {
                    Socket client = ss.accept();

                    new Thread(new ServiceThread(client, R, timeout, rebalance_period, commandQueue, fileIndex, storeIndex)).start();
                }
                catch(Exception e){System.out.println("error "+e);}
            }
        }
        catch(Exception e){System.out.println("error "+e);}
        System.out.println();
    }
}
