import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class Controller
{
    public static void main(String[] args)
    {
        try
        {
            int cport = Integer.parseInt(args[0]);
            int R = Integer.parseInt(args[1]);
            int timeout = Integer.parseInt(args[2]);
            int rebalance_period = Integer.parseInt(args[3]);

            ReentrantLock storeVectorChangeLock = new ReentrantLock(true);
            ReentrantLock rebalanceLock = new ReentrantLock(true);

            AtomicInteger storesInProg = new AtomicInteger(0);
            AtomicInteger removesInProg = new AtomicInteger(0);

            ServerSocket ss = new ServerSocket(cport);
            ConcurrentLinkedQueue<String[]> commandQueue = new ConcurrentLinkedQueue<String[]>();

            // Will be used for getting info about files, based on their filename
            ConcurrentHashMap<String, FileIndex> fileIndex = new ConcurrentHashMap<String, FileIndex>();

            // Will be used to keep track which files are "in progess" - either remove or store. Should work.
            Vector<String> fileIndexInProg = new Vector<String>();

            ConcurrentHashMap<String, Vector<Integer>> receivedStoreACKs = new ConcurrentHashMap<String, Vector<Integer>>();
            ConcurrentHashMap<String, Vector<Integer>> receivedRemoveACKs = new ConcurrentHashMap<String, Vector<Integer>>();
            Vector<Integer> receivedACKRebalances = new Vector<Integer>();

            // Will be used for quick access to stores, based on their socket number
            ConcurrentHashMap<Integer, Socket> storeIndex = new ConcurrentHashMap<Integer, Socket>();

            // Will be used to keep an order of which DStores are least "full" - used for properly storing new files
            Vector<DStoreIndex> storeVector = new Vector<DStoreIndex>();

            Vector<JoinDStoreInfoPair> waitingDStores = new Vector<JoinDStoreInfoPair>();

            AtomicLong lastRebalance = new AtomicLong(System.currentTimeMillis());

            new Thread(new ControllerRebalanceThread(R, timeout, rebalance_period, commandQueue, fileIndex, fileIndexInProg,
                    storeIndex, receivedStoreACKs, receivedRemoveACKs, storeVector, storeVectorChangeLock, rebalanceLock, storesInProg,
                    removesInProg, lastRebalance, waitingDStores, receivedACKRebalances)).start();

            for(;;)
            {
                try
                {
                    Socket client = ss.accept();

                    new Thread(new ControllerThread(client, R, timeout, rebalance_period, commandQueue, fileIndex, fileIndexInProg,
                            storeIndex, receivedStoreACKs, receivedRemoveACKs, storeVector, storeVectorChangeLock, rebalanceLock, storesInProg,
                            removesInProg, lastRebalance, waitingDStores, receivedACKRebalances)).start();
                }
                catch(Exception e){System.out.println("error22 "+e);}
            }
        }
        catch(Exception e){System.out.println("error "+e);}
        System.out.println();
    }
}
