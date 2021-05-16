/** Mine  */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

class ControllerRebalanceDStoreThread2 implements Runnable
{
    private int port;
    //private ConcurrentHashMap<String, Vector<Integer>> send;
    //private Vector<String> remove;
    private Vector<Integer> receivedACKRebalances;
    private Socket DStoreSocket;

    private BufferedReader bfin;
    private PrintWriter prout;

    public ControllerRebalanceDStoreThread2(int port_,
                                            Vector<Integer> receivedACKRebalances_, Socket DStoreSocket_)
    {
        port = port_;
        receivedACKRebalances = receivedACKRebalances_;
        DStoreSocket = DStoreSocket_;
    }

    public void run()
    {
        try
        {
            if(DStoreSocket.isClosed()) return;

            bfin = new BufferedReader(new InputStreamReader(DStoreSocket.getInputStream()));
            prout = new PrintWriter(DStoreSocket.getOutputStream(), true);

            String message = Protocol.REBALANCE_TOKEN;

            if(RebalanceInfo.commandsToSend.get(port) != null)
            {
                String[] filenamesSend = new String[RebalanceInfo.commandsToSend.get(port).size()];
                if (RebalanceInfo.commandsToSend.get(port).size() != 0) RebalanceInfo.commandsToSend.get(port).keySet().toArray(filenamesSend);


                message += " " + RebalanceInfo.commandsToSend.get(port).size();

                for (int i = 0; i < filenamesSend.length; i++)
                {
                    message += " " + filenamesSend[i];

                    Vector<Integer> ports = RebalanceInfo.commandsToSend.get(port).get(filenamesSend[i]);

                    message += " " + ports.size();

                    for (int y = 0; y < ports.size(); y++)
                    {
                        message += " " + ports.get(y);
                    }
                }
            }
            else message += " 0";

            if(RebalanceInfo.commandsToRemove.get(port) != null)
            {
                message += " " + RebalanceInfo.commandsToRemove.get(port).size();

                for (int i = 0; i < RebalanceInfo.commandsToRemove.get(port).size(); i++)
                {
                    message += " " + RebalanceInfo.commandsToRemove.get(port).get(i);
                }
            }
            else message += " 0";

            if(!DStoreSocket.isClosed())
            {
                prout.println(message);
            }
        }
        catch(Exception e) {System.out.println("uuh oh stinkyyyyy"); e.printStackTrace();}
    }
}