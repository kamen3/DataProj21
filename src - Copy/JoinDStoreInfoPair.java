import java.net.Socket;

/** Mine */
public class JoinDStoreInfoPair
{
    private int port;
    private Socket socket;

    public JoinDStoreInfoPair(int port_, Socket socket_)
    {
        port = port_;
        socket = socket_;
    }

    public int getPort()
    {
        return port;
    }

    public Socket getSocket()
    {
        return socket;
    }
}