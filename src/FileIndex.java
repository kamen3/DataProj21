import java.util.Vector;

/** Mine */
public class FileIndex
{
    private Integer filesize;
    private Vector<Integer> DStores; // the ports

    public FileIndex(int filesize_)
    {
        filesize = filesize_;
        DStores = new Vector<Integer>();
    }

    public Vector<Integer> getDStores()
    {
        return DStores;
    }

    public void addNewDStore(int port)
    {
        DStores.add(port);
    }

    public void removeDStore(Integer port)
    {
        DStores.remove((Object)port);
    }
}