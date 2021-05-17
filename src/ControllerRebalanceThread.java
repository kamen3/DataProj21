/** Mine  */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class ControllerRebalanceThread implements Runnable
{
    private int R;
    private int timeout;
    private int rebalance_period;
    private ConcurrentLinkedQueue<String[]> commandQueue; // Honestly probably unnecessary, the buffer in BufferedReader I think takes care of it
    private ConcurrentHashMap<String, FileIndex> fileIndex;
    private Vector<String> fileIndexInProg;
    private ConcurrentHashMap<Integer, Socket> storeIndex;
    private Vector<DStoreIndex> storeVector;
    private ConcurrentHashMap<String, Vector<Integer>> receivedStoreACKs, receivedRemoveACKs;
    Vector<Integer> receivedACKRebalances;

    private ConcurrentHashMap<String, Integer> loadAttempts;

    private ReentrantLock storeVectorChangeLock, rebalanceLock;
    private AtomicInteger storesInProg, removesInProg;

    private BufferedReader bfin;
    private PrintWriter prout;

    private AtomicLong lastRebalance;
    Vector<JoinDStoreInfoPair> waitingDStores;

    private String inpLine;
    private String[] comArgs;
    private String command;

    /** Should have just made it extend Controller, but oh well... */
    public ControllerRebalanceThread(int R_, int timeout_, int rebalance_period_, ConcurrentLinkedQueue<String[]> commandQueue_,
                                     ConcurrentHashMap<String, FileIndex> fileIndex_, Vector<String> fileIndexInProg_,
                                     ConcurrentHashMap<Integer, Socket> storeIndex_,
                                     ConcurrentHashMap<String, Vector<Integer>> receivedStoreACKs_,
                                     ConcurrentHashMap<String, Vector<Integer>> receivedRemoveACKs_,
                                     Vector<DStoreIndex> storeVector_, ReentrantLock storeVectorChangeLock_,
                                     ReentrantLock rebalanceLock_,
                                     AtomicInteger storesInProg_, AtomicInteger removesInProg_, AtomicLong lastRebalance_,
                                     Vector<JoinDStoreInfoPair> waitingDStores_,  Vector<Integer> receivedACKRebalances_)
    {
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
        storeVectorChangeLock = storeVectorChangeLock_;
        rebalanceLock = rebalanceLock_;
        storesInProg = storesInProg_;
        removesInProg = removesInProg_;
        lastRebalance = lastRebalance_;
        waitingDStores = waitingDStores_;
        receivedACKRebalances = receivedACKRebalances_;

        loadAttempts = new ConcurrentHashMap<String, Integer>();
    }

    public void run()
    {
        try
        {
            for(;;)
            {
                if((System.currentTimeMillis() - lastRebalance.get() >= rebalance_period || waitingDStores.size()>0))
                {
                    if (storeVectorChangeLock.tryLock(1, TimeUnit.DAYS)) // Just wait until it's available
                    {
                        while(storesInProg.get() != 0 && removesInProg.get() != 0)
                        {
                            /** Wait for all STORE and REMOVE operations to cease */
                        }

                        JoinDStoreInfoPair[] waitingDStoresInfos = new JoinDStoreInfoPair[waitingDStores.size()];
                        waitingDStores.toArray(waitingDStoresInfos);
                        waitingDStores.removeAllElements();

                        for(int i=0; i<waitingDStoresInfos.length; i++)
                        {
                            storeIndex.put(waitingDStoresInfos[i].getPort(), waitingDStoresInfos[i].getSocket());
                            storeVector.add(new DStoreIndex(waitingDStoresInfos[i].getPort()));
                        }

                        if(storeVector.size() < R)
                        {
                            lastRebalance.set(System.currentTimeMillis());
                            storeVectorChangeLock.unlock();
                            continue;
                        }

                        Vector<Integer> closedDStores = new Vector<Integer>();
                        Vector<String> disappearedFiles = new Vector<String>();
                        RebalanceInfo.currentDStoreList.clear();
                        RebalanceInfo.currentTotalFileList.clear();
                        RebalanceInfo.commandsToSend.clear();
                        RebalanceInfo.commandsToRemove.clear();
                        RebalanceInfo.filesToReceive.clear();

                        for(int i=0; i<storeVector.size(); i++)
                        {
                            if(storeIndex.get(storeVector.get(i).getPort()).isClosed()) closedDStores.add(i);
                            else
                            {
                                long startTime = System.currentTimeMillis();
                                AtomicBoolean flag = new AtomicBoolean(true);

                                new Thread(new ControllerRebalanceDStoreThread(storeIndex.get(storeVector.get(i).getPort()),
                                        storeVector.get(i).getPort(), flag)).start();

                                while(System.currentTimeMillis() - startTime <= timeout)
                                {
                                    if(!flag.get()) break;
                                }

                                if(flag.get())
                                {
                                    storeIndex.get(storeVector.get(i).getPort()).close(); // that should hopefully stop any incoming info
                                    closedDStores.add(i);
                                }
                            }
                        }
                        for(int i=0; i<closedDStores.size(); i++)
                        {
                            storeIndex.remove(closedDStores.get(i));
                        }
                        Vector<DStoreIndex> stableDStores = new Vector<DStoreIndex>();
                        for(int i=0; i<storeVector.size(); i++)
                        {
                            DStoreIndex el = storeVector.get(i);
                            if(!storeIndex.get(el.getPort()).isClosed()) stableDStores.add(el);
                        }
                        storeVector.clear();
                        storeVector.addAll(stableDStores);

                        if(storeVector.size() < R)
                        {
                            lastRebalance.set(System.currentTimeMillis());
                            storeVectorChangeLock.unlock();
                            continue;
                        }

                        for(int i=0; i<storeVector.size(); i++)
                        {
                            int curDStore = storeVector.get(i).getPort();
                            Vector<String> actualFiles = RebalanceInfo.currentDStoreList.get(curDStore);

                            if (actualFiles == null) continue;
                            System.out.println("1 Port " + curDStore + " has " + actualFiles.size() + ", but in the vector it's " + storeVector.get(i).getNumFiles());
                            //System.out.println(actualFiles.toString());
                        }




                        /** So check if there's any files below or above R, and fill up the numbers by redistributing them randomly,
                         *  and do that by simply starting to build the messages early and including that info there
                         */
                        ConcurrentHashMap.KeySetView<String, FileIndex> inFileIndex_ = fileIndex.keySet();
                        String[] inFileIndex = new String[inFileIndex_.size()];
                        inFileIndex_.toArray(inFileIndex);
                        for(int i=0; i<inFileIndex.length; i++)
                        {
                            if(!RebalanceInfo.currentTotalFileList.containsKey(inFileIndex[i])) disappearedFiles.add(inFileIndex[i]);
                            else if(RebalanceInfo.currentTotalFileList.get(inFileIndex[i]).size() < R)
                            {
                                String file = inFileIndex[i];
                                int curNum = RebalanceInfo.currentTotalFileList.get(file).size();

                                for(int y=0; y<storeVector.size() && curNum<R; y++)
                                {
                                    if(!storeVector.get(y).getFiles().contains(file))
                                    {
                                        curNum++;
                                        DStoreIndex DStoreTo = storeVector.get(y);
                                        DStoreTo.addFile(file);
                                        System.out.println("addd1");
                                        int from = RebalanceInfo.currentTotalFileList.get(file).get(new Random().nextInt(RebalanceInfo.currentTotalFileList.get(file).size()));
                                        if(!RebalanceInfo.commandsToSend.containsKey(from))
                                        {
                                            // Clone the file to some random DStore that doesn't have it - do this merely by commmand,
                                            // but act like it's real
                                            ConcurrentHashMap<String, Vector<Integer>> tmp = new ConcurrentHashMap<String, Vector<Integer>>();
                                            Vector<Integer> tmp2 = new Vector<Integer>();
                                            tmp2.add(DStoreTo.getPort());
                                            tmp.put(file, tmp2);
                                            RebalanceInfo.commandsToSend.put(from, tmp);
                                            System.out.println("send10");
                                        }
                                        else if(!RebalanceInfo.commandsToSend.get(from).containsKey(file))
                                        {
                                            Vector<Integer> tmp = new Vector<Integer>();
                                            tmp.add(DStoreTo.getPort());
                                            RebalanceInfo.commandsToSend.get(from).put(file, tmp);
                                            System.out.println("send9");
                                        }
                                        else
                                        {
                                            RebalanceInfo.commandsToSend.get(from).get(file).add(DStoreTo.getPort());
                                            System.out.println("send8");
                                        }

                                        if(!RebalanceInfo.filesToReceive.containsKey(DStoreTo.getPort()))
                                        {
                                            Vector<String> tmp = new Vector<String>();
                                            tmp.add(file);
                                            RebalanceInfo.filesToReceive.put(DStoreTo.getPort(), tmp);
                                        }
                                        else
                                        {
                                            RebalanceInfo.filesToReceive.get(DStoreTo.getPort()).add(file);
                                        }
                                    }
                                }
                            }
                            else if(RebalanceInfo.currentTotalFileList.get(inFileIndex[i]).size() > R)
                            {
                                String file = inFileIndex[i];
                                int curNum = RebalanceInfo.currentTotalFileList.get(file).size();

                                for(int y=0; y<storeVector.size() && curNum>R; y++)
                                {
                                    if(storeVector.get(y).getFiles().contains(file))
                                    {
                                        curNum--;
                                        DStoreIndex DStoreTo = storeVector.get(y);
                                        DStoreTo.removeFile(file);
                                        if(!RebalanceInfo.commandsToRemove.containsKey(DStoreTo.getPort()))
                                        {
                                            Vector<String> tmp = new Vector<String>();
                                            tmp.add(file);
                                            RebalanceInfo.commandsToRemove.put(DStoreTo.getPort(), tmp);
                                            System.out.println("rem8");
                                        }
                                        else
                                        {
                                            RebalanceInfo.commandsToRemove.get(DStoreTo.getPort()).add(file);
                                            System.out.println("rem7");
                                        }
                                    }
                                }
                            }
                        }
                        for(int i=0; i<disappearedFiles.size(); i++)
                        {
                            fileIndex.remove(disappearedFiles.get(i));
                        }
                        for(int i=0; i<storeVector.size(); i++)
                        {
                            storeVector.get(i).removeFiles(disappearedFiles);
                        }

                        /**
                         * Also would be good to check if there's any files that /shouldn't/ be in the folder,
                         * i.e. that aren't in the fileIndex
                         */
                        for(int i=0; i<storeVector.size(); i++)
                        {
                            int curDStore = storeVector.get(i).getPort();
                            Vector<String> actualFiles = RebalanceInfo.currentDStoreList.get(curDStore);

                            if(actualFiles == null) continue;
                            //System.out.println("2 Port " + curDStore + " has " + actualFiles.size() + ", but in the vector it's " + storeVector.get(i).getNumFiles());
                            for(int y=0; y<actualFiles.size(); y++)
                            {
                                String file = actualFiles.get(y);

                                if(!fileIndex.containsKey(file))
                                {
                                    if(!RebalanceInfo.commandsToRemove.containsKey(curDStore))
                                    {
                                        Vector<String> tmp = new Vector<String>();
                                        tmp.add(file);
                                        RebalanceInfo.commandsToRemove.put(curDStore, tmp);
                                        System.out.println("rem6");
                                    }
                                    else
                                    {
                                        RebalanceInfo.commandsToRemove.get(curDStore).add(file);
                                        System.out.println("rem5");
                                    }
                                }
                            }
                        }
                        System.out.println("passes the loop");

                        /** So now take from the ones that have the most files and redistribute to the ones that
                         *  have the least (by picking randomly?)
                         *
                         * Hmmmm...
                         * There might be a problem if we try sending a file we have not received yet
                         * It kinds says to just ignore and move on, later rebalances will fix, okay
                         *
                         * Actually just make another structure that keeps track of sensitive "to-be-received"
                         * files
                         *
                         * And just don't pick files that are to be received by the DStore, pick some other random one.
                         * There should be guaranteed a choice, cuz otherwise it'd be really weird????
                         * */
                        balance();
                        balance(); /** Do twice to smoothe thigns out in case of an odd number of DStores */

                        receivedACKRebalances.removeAllElements();

                        for(int i=0; i<storeVector.size(); i++)
                        {
                            int port = storeVector.get(i).getPort();
                            new Thread(new ControllerRebalanceDStoreThread2(port,
                                    receivedACKRebalances,
                                    storeIndex.get(storeVector.get(i).getPort()))).start();
                        }

                        long startTime = System.currentTimeMillis();
                        Boolean flag=true;
                        while((System.currentTimeMillis() - startTime) <= timeout)
                        {
                            if (receivedACKRebalances.size() == storeVector.size())
                            {
                                flag = false;
                                break;
                            }
                        }
                        if(flag)
                        {
                            /** If timed out, do nothing */
                            System.out.println("Timed out rebalancing");

                        }

                        /**
                         * Will have to do thread splitting when sending the finalized commands to the DStores
                         * and look for timeout there as well
                         * **/

                        /**
                         * Check carefully if you're handling the fileIndex, storeVector etc collection properly
                         * and they have accurate information at the end of it all
                         *
                         * One thought I had was to simply rebuild them?
                         * But then if DStores fail to do their job properly?
                         * Think carefully
                         * */

                        lastRebalance.set(System.currentTimeMillis());
                        storeVectorChangeLock.unlock();
                        System.out.println("==========================");
                    }
                }
            }
        }
        catch(Exception e) {System.out.println("uuh oh stinkyyyyy3"); e.printStackTrace();}
    }

    private void balance()
    {
        storeVector.sort(null);

        //System.out.println("Before balance: " + storeVector.toString());

        for(int i=0; i<storeVector.size()/2; i++)
        {
            DStoreIndex lowDStore = storeVector.get(i);
            DStoreIndex highDStore = storeVector.get(storeVector.size()-1-i);
            int lowNum = lowDStore.getNumFiles();
            int highNum = highDStore.getNumFiles();
            int lowPort = lowDStore.getPort();
            int highPort = highDStore.getPort();
            Vector<String> highDStoreFiles = highDStore.getFiles();
            Vector<String> toSend = new Vector<String>();

            while(highNum - lowNum > 1)
            {
                for (;;)
                {
                    String file = highDStoreFiles.get(new Random().nextInt(highDStoreFiles.size()));
                    int tmpport = highDStore.getPort();

                    if(!RebalanceInfo.filesToReceive.containsKey(tmpport) && !toSend.contains(file))
                    {
                        if(lowDStore.getFiles().contains(file)) continue;

                        lowDStore.addFile(file);
                        highDStore.removeFile(file);
                        toSend.add(file);
                        lowNum++;
                        highNum--;

                        if(!RebalanceInfo.commandsToSend.containsKey(highPort))
                        {
                            ConcurrentHashMap<String, Vector<Integer>> tmp = new ConcurrentHashMap<String, Vector<Integer>>();
                            Vector<Integer> tmp2 = new Vector<Integer>();
                            tmp2.add(lowPort);
                            tmp.put(file,tmp2);
                            RebalanceInfo.commandsToSend.put(highPort, tmp);
                            System.out.println("send6");
                        }
                        else if(!RebalanceInfo.commandsToSend.get(highPort).containsKey(file))
                        {
                            Vector<Integer> tmp = new Vector<Integer>();
                            tmp.add(lowPort);
                            RebalanceInfo.commandsToSend.get(highPort).put(file, tmp);
                            System.out.println("send5");
                        }
                        else
                        {
                            RebalanceInfo.commandsToSend.get(highPort).get(file).add(lowPort);
                            System.out.println("send4");
                        }

                        if(!RebalanceInfo.commandsToRemove.containsKey(highPort))
                        {
                            Vector<String> tmp = new Vector<String>();
                            tmp.add(file);
                            RebalanceInfo.commandsToRemove.put(highPort, tmp);
                            System.out.println("rem4");
                        }
                        else
                        {
                            RebalanceInfo.commandsToRemove.get(highPort).add(file);
                            System.out.println("rem3");
                        }

                        break;
                    }

                    else if (!RebalanceInfo.filesToReceive.get(tmpport).contains(file) && !toSend.contains(file))
                    {
                        if(lowDStore.getFiles().contains(file)) continue;

                        lowDStore.addFile(file);
                        highDStore.removeFile(file);
                        toSend.add(file);
                        lowNum++;
                        highNum--;

                        if(!RebalanceInfo.commandsToSend.containsKey(highPort))
                        {
                            ConcurrentHashMap<String, Vector<Integer>> tmp = new ConcurrentHashMap<String, Vector<Integer>>();
                            Vector<Integer> tmp2 = new Vector<Integer>();
                            tmp2.add(lowPort);
                            tmp.put(file,tmp2);
                            RebalanceInfo.commandsToSend.put(highPort, tmp);
                            System.out.println("send3");
                        }
                        else if(!RebalanceInfo.commandsToSend.get(highPort).containsKey(file))
                        {
                            Vector<Integer> tmp = new Vector<Integer>();
                            tmp.add(lowPort);
                            RebalanceInfo.commandsToSend.get(highPort).put(file, tmp);
                            System.out.println("send2");
                        }
                        else
                        {
                            System.out.println("send1");
                            RebalanceInfo.commandsToSend.get(highPort).get(file).add(lowPort);
                        }

                        if(!RebalanceInfo.commandsToRemove.containsKey(highPort))
                        {
                            Vector<String> tmp = new Vector<String>();
                            tmp.add(file);
                            RebalanceInfo.commandsToRemove.put(highPort, tmp);
                            System.out.println("rem2");
                        }
                        else
                        {
                            RebalanceInfo.commandsToRemove.get(highPort).add(file);
                            System.out.println("rem1");
                        }

                        break;
                    }
                }
            }
        }

        //System.out.println("After balance: " + storeVector.toString());
    }
}