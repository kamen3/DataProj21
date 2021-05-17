import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public enum RebalanceInfo
{
    INSTANCE("Let's hope it works");

    public static ConcurrentHashMap<String, Vector<Integer>> currentTotalFileList = new ConcurrentHashMap<String, Vector<Integer>>();
    public static ConcurrentHashMap<Integer, Vector<String>> currentDStoreList = new ConcurrentHashMap<Integer, Vector<String>>();
    public static ConcurrentHashMap<Integer, ConcurrentHashMap<String, Vector<Integer>>> commandsToSend = new ConcurrentHashMap<Integer, ConcurrentHashMap<String, Vector<Integer>>>();
    public static ConcurrentHashMap<Integer, Vector<String>> commandsToRemove= new ConcurrentHashMap<Integer, Vector<String>>();
    public static ConcurrentHashMap<Integer, Vector<String>> filesToReceive = new ConcurrentHashMap<Integer, Vector<String>>();

    private RebalanceInfo(String info)
    { }
}
