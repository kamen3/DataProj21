/** Mine  */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

class ControllerThread implements Runnable
{
    private Socket client;
    private int R;
    private int timeout;
    private int rebalance_period;
    private ConcurrentLinkedQueue<String[]> commandQueue; // Honestly probably unnecessary, the buffer in BufferedReader I think takes care of it
    private ConcurrentHashMap<String, FileIndex> fileIndex;
    private Vector<String> fileIndexInProg;
    private ConcurrentHashMap<Integer, Socket> storeIndex;
    private Vector<DStoreIndex> storeVector;
    private ConcurrentHashMap<String, Vector<Integer>> receivedStoreACKs;

    ReentrantLock loadLock, storeLock, removeLock, rebalanceLock;
    AtomicInteger storesInProg;

    BufferedReader bfin;
    PrintWriter prout;

    String inpLine;
    String[] comArgs;
    String command;

    public ControllerThread(Socket client_, int R_, int timeout_, int rebalance_period_, ConcurrentLinkedQueue<String[]> commandQueue_,
                            ConcurrentHashMap<String, FileIndex> fileIndex_, Vector<String> fileIndexInProg_,
                            ConcurrentHashMap<Integer, Socket> storeIndex_,
                            ConcurrentHashMap<String, Vector<Integer>> receivedStoreACKs_,
                            Vector<DStoreIndex> storeVector_, ReentrantLock loadLock_, ReentrantLock storeLock_,
                            ReentrantLock removeLock_, ReentrantLock rebalanceLock_,
                            AtomicInteger storesInProg_)
    {
        client = client_;
        R = R_;
        timeout = timeout_;
        rebalance_period = rebalance_period_;
        commandQueue = commandQueue_;
        fileIndex = fileIndex_;
        fileIndexInProg = fileIndexInProg_;
        storeIndex = storeIndex_;
        receivedStoreACKs = receivedStoreACKs_;
        storeVector = storeVector_;
        loadLock = loadLock_;
        storeLock = storeLock_;
        removeLock = removeLock_;
        rebalanceLock = rebalanceLock_;
        storesInProg = storesInProg_;
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

                /** Based on the first command, probably split behaviour into two methods:
                 *  one for client
                 *  one for store
                 *  */
                if(command.equals(Protocol.JOIN_TOKEN)) actOnJoin(); // DStore
                else if(command.equals(Protocol.LIST_TOKEN)) actOnList(); // Client
                else if(command.equals(Protocol.STORE_TOKEN)) actOnStore(); // Client
                else if(command.equals(Protocol.STORE_ACK_TOKEN)) actOnStoreAck(); // DStore
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
                return;
            }

            /** Should I check for timeout here??????? */
            String message = "LIST";

            if(!fileIndex.keySet().isEmpty())
            {
                String[] files = fileIndex.keySet().toString().split(", ");

                for (String name : files)
                {
                    message += " " + name.substring(1, name.length() - 2);
                }
            }

            prout.println(message);
        }
        catch(Exception e){System.out.println("uh oh stinky2");}
    }

    private void actOnStore()
    {
        if (storeIndex.size() < R)
        {
            prout.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        if(comArgs.length != 3)
        {
            /** Log later */
            return;
        }

        String filename = comArgs[1];
        int filesize = Integer.parseInt(comArgs[2]);

        if(fileIndex.containsKey(filename) || fileIndexInProg.contains(filename) )
        {
            prout.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
            return;
        }

        storesInProg.incrementAndGet();

        fileIndexInProg.add(filename);

        try
        {
            if (storeLock.tryLock(1, TimeUnit.DAYS)) // Just wait until it's available
            {
                storeVector.sort(null); // Sort to be able to get the least full DStores
                DStoreIndex[] curDStores = (DStoreIndex[]) storeVector.toArray();
                String message = Protocol.STORE_TO_TOKEN;
                Vector<DStoreIndex> newVec = new Vector<DStoreIndex>();
                FileIndex fileInfo = new FileIndex(filesize);

                for (int i = 0; i < R; i++)
                {
                    message += " " + Integer.toString(curDStores[i].getPort());
                    curDStores[i].addFile(new FileInfoPair(filename, filesize));
                    newVec.add(curDStores[i]);
                    fileInfo.addNewDStore(curDStores[i].getPort());
                }
                for (int i = R; i < curDStores.length; i++)
                {
                    newVec.add(curDStores[i]);
                }

                storeVector.clear();
                storeVector.addAll(newVec);

                /** Release the lock */
                storeLock.unlock();

                receivedStoreACKs.put(filename, new Vector<Integer>());

                prout.println(message);

                Thread.sleep(timeout);

                if (receivedStoreACKs.get(filename).size() == R)
                {
                    prout.println(Protocol.STORE_COMPLETE_TOKEN);
                    receivedStoreACKs.remove(filename); // Ditto?
                    fileIndexInProg.remove(filename); // By leaving it here otherwise, it could be used to know it should be removed in rebalancing!
                    fileIndex.put(filename, fileInfo);
                }
                /** Otherwise, do nothing */

                storesInProg.decrementAndGet();
            }
        }
        catch(Exception e) {}
    }

    public void actOnStoreAck()
    {
        receivedStoreACKs.get(comArgs[1]).add(client.getPort()); // Should work??????? although tbf it might not matter
    }
}