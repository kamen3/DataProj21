/** Mine  */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

class ControllerRebalanceDStoreThread implements Runnable
{
    ConcurrentHashMap<Integer, Vector<String>> currentDStoreList;
    ConcurrentHashMap<String, Vector<Integer>> currentTotalFileList;
    Socket DStoreSocket;
    int port;

    private BufferedReader bfin;
    private PrintWriter prout;

    private String inpLine;
    private String[] comArgs;
    private String command;

    private AtomicBoolean flag;

    /** Should have just made it extend Controller, but oh well... */
    public ControllerRebalanceDStoreThread(ConcurrentHashMap<Integer, Vector<String>> currentDStoreList_,
                                           ConcurrentHashMap<String, Vector<Integer>> currentTotalFileList_,
                                           Socket DStoreSocket_, int port_, AtomicBoolean flag_)
    {
        currentDStoreList = currentDStoreList_;
        currentTotalFileList = currentTotalFileList_;
        DStoreSocket = DStoreSocket_;
        port = port_;
        flag = flag_;
    }

    public void run()
    {
        try
        {
            bfin = new BufferedReader(new InputStreamReader(DStoreSocket.getInputStream()));
            prout = new PrintWriter(DStoreSocket.getOutputStream(), true);

            prout.println(Protocol.LIST_TOKEN);

            inpLine = bfin.readLine();
            comArgs = inpLine.split(" ");

            Vector<String> files = new Vector<String>();
            for(int i=1; i<comArgs.length; i++)
            {
                String filename = comArgs[i];
                files.add(filename);
                if(!currentTotalFileList.containsKey(filename))
                {
                    Vector<Integer> tmp = new Vector<Integer>();
                    tmp.add(port);
                    currentTotalFileList.put(filename, tmp);
                }
                else currentTotalFileList.get(filename).add(port);
            }

            currentDStoreList.put(port, files);

            flag.set(false);

            bfin.close();
            prout.close();
        }
        catch(Exception e) {System.out.println("uuh oh stinkyyyyy"); e.printStackTrace();}
    }
}