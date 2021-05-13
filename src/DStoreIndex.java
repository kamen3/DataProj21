import java.net.Socket;
import java.util.Vector;

/** Mine */
public class DStoreIndex implements Comparable
{
    private int port;
    private Vector<FileInfoPair> files;
    //private Socket socket; unnecessary - can be looked up in storeIndex using the port number

    public DStoreIndex(int port_)
    {
        port = port_;
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

    @Override
    public int compareTo(Object o)
    {
        int osize = ((DStoreIndex)o).getNumFiles();

        if(files.size() > osize) return 1;
        if(files.size() < osize) return -1;
        return 0;
    }
}