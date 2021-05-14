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
    private ConcurrentHashMap<String, Vector<Integer>> receivedStoreACKs, receivedRemoveACKs;

    ReentrantLock loadLock, storeVectorChangeLock, rebalanceLock;
    AtomicInteger storesInProg, removesInProg;

    BufferedReader bfin;
    PrintWriter prout;

    String inpLine;
    String[] comArgs;
    String command;

    /** Should have just made it extend Controller, but oh well... */
    public ControllerThread(Socket client_, int R_, int timeout_, int rebalance_period_, ConcurrentLinkedQueue<String[]> commandQueue_,
                            ConcurrentHashMap<String, FileIndex> fileIndex_, Vector<String> fileIndexInProg_,
                            ConcurrentHashMap<Integer, Socket> storeIndex_,
                            ConcurrentHashMap<String, Vector<Integer>> receivedStoreACKs_,
                            ConcurrentHashMap<String, Vector<Integer>> receivedRemoveACKs_,
                            Vector<DStoreIndex> storeVector_, ReentrantLock loadLock_, ReentrantLock storeVectorChangeLock_,
                            ReentrantLock rebalanceLock_,
                            AtomicInteger storesInProg_, AtomicInteger removesInProg_)
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
        receivedRemoveACKs = receivedRemoveACKs_;
        storeVector = storeVector_;
        loadLock = loadLock_;
        storeVectorChangeLock = storeVectorChangeLock_;
        rebalanceLock = rebalanceLock_;
        storesInProg = storesInProg_;
        removesInProg = removesInProg_;
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
                else if(command.equals(Protocol.REMOVE_TOKEN)) actOnRemove(); // Client
                else if(command.equals(Protocol.REMOVE_ACK_TOKEN)) actOnRemoveAck(); // DStore
                else if(command.equals(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN)) actOnErrorFileNotExist(); // DStore
                else System.out.println("unrecognised command");// Will probably have to call logger here
            }

            bfin.close();
            prout.close();
            client.close();
        }
        catch(Exception e) {System.out.println("uuh oh stinkyyyyy"); e.printStackTrace();}
    }

    private void actOnJoin()
    {
        storeIndex.put(Integer.parseInt(comArgs[1]), client);
        storeVector.add(new DStoreIndex(Integer.parseInt(comArgs[1])));
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

            String message = "LIST";
            int start, end;

            if(!fileIndex.keySet().isEmpty())
            {
                String[] files = fileIndex.keySet().toString().split(", ");

                for (String name : files)
                {
                    start = 0;
                    if(name.charAt(0) == '[') start++;
                    end = name.length() - 1;
                    if(name.charAt(name.length() - 1) != ']') end++;
                    message += " " + name.substring(start, end);
                }
            }
            prout.println(message);
        }
        catch(Exception e){System.out.println("uh oh stinky2"); e.printStackTrace();}
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

        /** In rebalancing, will have to make the thread snatch all the locks, lol */

        try
        {
            if (storeVectorChangeLock.tryLock(1, TimeUnit.DAYS)) // Just wait until it's available
            {
                storeVector.sort(null); // Sort to be able to get the least full DStores
                String message = Protocol.STORE_TO_TOKEN;
                FileIndex fileInfo = new FileIndex(filesize);

                for (int i = 0; i < R; i++)
                {
                    message += " " + Integer.toString(storeVector.get(i).getPort());
                    storeVector.get(i).addFile(filename, filesize);
                    fileInfo.addNewDStore(storeVector.get(i).getPort());
                }

                /** Release the lock */
                storeVectorChangeLock.unlock();

                receivedStoreACKs.put(filename, new Vector<Integer>());

                prout.println(message);

                long startTime = System.currentTimeMillis();
                Boolean flag=true;
                while((System.currentTimeMillis() - startTime) <= timeout)
                {
                    if (receivedStoreACKs.get(filename).size() == R)
                    {
                        flag = false;
                        prout.println(Protocol.STORE_COMPLETE_TOKEN);
                        receivedStoreACKs.remove(filename); // Ditto?
                        fileIndexInProg.remove(filename); // By leaving it here otherwise, it could be used to know it should be removed in rebalancing!
                        fileIndex.put(filename, fileInfo);
                        break;
                    }
                }
                if(flag)
                {
                    /** If timed out, do nothing */
                    System.out.println("Timed out storing");
                }

                storesInProg.decrementAndGet();
            }
        }
        catch(Exception e) {System.out.println("stinkyyy"); e.printStackTrace();}
    }

    private void actOnStoreAck()
    {
        receivedStoreACKs.get(comArgs[1]).add(client.getPort()); // Should work??????? although tbf it might not matter
    }

    private void actOnRemove()
    {
        if (storeIndex.size() < R)
        {
            prout.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
            return;
        }

        if(comArgs.length != 2)
        {
            /** Log later */
            return;
        }

        String filename = comArgs[1];

        if(!fileIndex.containsKey(filename) || fileIndexInProg.contains(filename) )
        {
            prout.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
            return;
        }

        removesInProg.incrementAndGet();

        FileIndex fileInfo = fileIndex.get(filename);
        fileIndex.remove(filename);
        fileIndexInProg.add(filename);

        try
        {
            if (storeVectorChangeLock.tryLock(1, TimeUnit.DAYS)) // Just wait until it's available
            {
                for(int i=0; i<storeVector.size(); i++)
                {
                    storeVector.get(i).removeFile(filename);
                }

                /** Release the lock */
                storeVectorChangeLock.unlock();

                Vector<Integer> DStorePorts = fileInfo.getDStores();

                receivedRemoveACKs.put(filename, new Vector<Integer>());
                PrintWriter DStorePrWr;

                for(int i=0; i<DStorePorts.size(); i++)
                {
                    DStorePrWr = new PrintWriter(storeIndex.get(DStorePorts.get(i)).getOutputStream(), true);
                    DStorePrWr.println(Protocol.REMOVE_TOKEN + " " + filename);
                }

                long startTime = System.currentTimeMillis();
                Boolean flag=true;
                while((System.currentTimeMillis() - startTime) <= timeout)
                {
                    if (receivedRemoveACKs.get(filename).size() == R)
                    {
                        flag = false;
                        fileIndexInProg.remove(filename); // By leaving it here otherwise, it could be used to know it should be removed in rebalancing!
                        break;
                    }
                }

                if(flag)
                {
                    System.out.println("Timed out removing");
                    /** If timed out, "log error" */
                }

                prout.println(Protocol.REMOVE_COMPLETE_TOKEN);
                receivedRemoveACKs.remove(filename); // Ditto?

                removesInProg.decrementAndGet();
            }
        }
        catch(Exception e) {System.out.println("stinkyyy2"); e.printStackTrace();}
    }

    private void actOnRemoveAck()
    {
        receivedRemoveACKs.get(comArgs[1]).add(client.getPort()); // Should work??????? although tbf it might not matter
    }

    private void actOnErrorFileNotExist()
    {
        /** Logging perhaps which DStore threw the error*/
        //receivedRemoveACKs.get(comArgs[1]).add(client.getPort());
        // I mean, if it already doesn't exist, no reason to forbid others from storing a file with the same name
    }
}