/** Mine  */
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.HashMap;
import java.util.Vector;

class ControllerRebalanceDStoreThread2 implements Runnable
{
    private int port;
    private HashMap<String, Vector<Integer>> send;
    Vector<String> remove;
    Vector<Integer> receivedACKRebalances;
    Socket DStoreSocket;

    private BufferedReader bfin;
    private PrintWriter prout;

    public ControllerRebalanceDStoreThread2(int port_, HashMap<String, Vector<Integer>> send_, Vector<String> remove_,
                                            Vector<Integer> receivedACKRebalances_, Socket DStoreSocket_)
    {
        port = port_;
        send = send_;
        remove = remove_;
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

            if(send != null)
            {
                String[] filenamesSend = new String[send.size()];
                if (send.size() != 0) send.keySet().toArray(filenamesSend);

                message += " " + send.size();

                for (int i = 0; i < filenamesSend.length; i++)
                {
                    message += " " + filenamesSend[i];

                    Vector<Integer> ports = send.get(filenamesSend[i]);

                    message += " " + ports.size();

                    for (int y = 0; y < ports.size(); y++)
                    {
                        message += " " + ports.get(y);
                    }
                }
            }
            else message += " 0";

            if(remove != null)
            {
                message += " " + remove.size();

                for (int i = 0; i < remove.size(); i++)
                {
                    message += " " + remove.get(i);
                }
            }
            else message += " 0";

            prout.println(message);
        }
        catch(Exception e) {System.out.println("uuh oh stinkyyyyy"); e.printStackTrace();}
    }
}