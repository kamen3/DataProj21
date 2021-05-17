import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/** Mine */
public class DStoreIndex implements Comparable
{
    private int port;
    private Vector<String> files;
    //private Socket socket; unnecessary - can be looked up in storeIndex using the port number

    public DStoreIndex(int port_)
    {
        port = port_;
        files = new Vector<String>();
    }

    public void addFile(String filename) { files.add(filename); }

    public void removeFile(String filename) { files.remove(filename); }

    public void removeFiles(Vector<String> filenames) { files.removeAll(filenames); }

    public int getPort()
    {
        return port;
    }

    public int getNumFiles()
    {
        return files.size();
    }

    public Vector<String> getFiles()
    {
        return files;
    }

    public String toString()
    {
        return (port + ">" + files.size());
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