import java.net.Socket;
import java.util.Vector;

/** Mine */
public class DStoreIndex implements Comparable
{
    private int port;
    private Vector<FileInfoPair> files;
    private Socket socket;

    public DStoreIndex(int port_, Socket socket_)
    {
        port = port_;
        socket = socket_;
        files = new Vector<FileInfoPair>();
    }

    public void addFile(FileInfoPair f)
    {
        files.add(f);
    }

    public int getPort()
    {
        return port;
    }

    public int getNumFiles()
    {
        return files.size();
    }

    public Vector<FileInfoPair> getFiles()
    {
        return files;
    }

    public Socket getSocket() { return socket; }

    @Override
    public int compareTo(Object o)
    {
        int osize = ((DStoreIndex)o).getNumFiles();

        if(files.size() > osize) return 1;
        if(files.size() < osize) return -1;
        return 0;
    }
}