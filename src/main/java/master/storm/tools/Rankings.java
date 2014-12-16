package master.storm.tools;

import java.util.HashMap;
import java.util.List;

/**
 *
 * @author fran
 */
public class Rankings {
    
    // Contains the top k topics
    private final HashMap<String, List<String>> rankings
            = new HashMap<String, List<String>>();
    private final int maxSize;
    
    public Rankings () {
        this.maxSize = 3;
    }
    
    public HashMap<String,List<String>> getRankings() {
        return rankings;
    }
    
    public void incrementCount(String country, String topic) {
        //We search for that country and word and update the rank list accordingly
    }

}
