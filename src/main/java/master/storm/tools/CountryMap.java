package master.storm.tools;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 * Esta clase representa un mapa de topics de un pais
 * @author fran
 */
public class CountryMap {

    private TreeMap<String, Word> topics;

    public CountryMap(TreeMap<String, Word> topics) {
        this.topics = topics;
    }

    public void insertWord(String key) {
        if (topics.containsKey(key)) {
            Word word = topics.get(key);
            word.increment();
            topics.put(key, word);
        } else {
            Word word = new Word(key, 1);
            topics.put(key, word);
        }
    }

    public ArrayList<String> getOrderedMap() {
        ArrayList<Word> orderedList = new ArrayList<>();
        for (Map.Entry<String, Word> entry : topics.entrySet()) {
            orderedList.add(entry.getValue());
        }
        orderedList.sort(null);
        ArrayList<String> returnList = new ArrayList<>();
        returnList.add(orderedList.get(0).getTopic());
        returnList.add(orderedList.get(1).getTopic());
        returnList.add(orderedList.get(2).getTopic());
        return returnList;
    }

    public TreeMap<String, Word> getTopics() {
        return topics;
    }

}
