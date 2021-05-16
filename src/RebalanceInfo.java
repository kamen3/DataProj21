import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class RebalanceInfo
{
    public static ConcurrentHashMap<Integer, Vector<String>> currentDStoreList = new ConcurrentHashMap<Integer, Vector<String>>();
    public static ConcurrentHashMap<String, Vector<Integer>> currentTotalFileList = new ConcurrentHashMap<String, Vector<Integer>>();
    public static ConcurrentHashMap<Integer, HashMap<String, Vector<Integer>>> commandsToSend = new ConcurrentHashMap<Integer, HashMap<String, Vector<Integer>>>();
    public static ConcurrentHashMap<Integer, Vector<String>> commandsToRemove= new ConcurrentHashMap<Integer, Vector<String>>();
    public static ConcurrentHashMap<Integer, Vector<String>> filesToReceive = new ConcurrentHashMap<Integer, Vector<String>>();
}
