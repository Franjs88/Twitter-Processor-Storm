package master.storm.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author fran
 */
public class Rankings {

    // Contains the top 3 topics by country
    private final HashMap<String, CountryMap> rankings
            = new HashMap<String, CountryMap>();

    public Rankings() {

    }

    public HashMap<String, CountryMap> getRankings() {
        return rankings;
    }

    public void incrementCount(String country, String topic) {
        //We search for that country and word and update the rank list accordingly
        CountryMap map = this.rankings.get(country);
        if (map == null) {
            map = new CountryMap(new TreeMap<String, Word>());
            rankings.put(country, map);
        }
        map.insertWord(topic);
    }

    public void writeOrdered() throws IOException {
        
        File fout = new File("Fco.JavierSanchezCarmona.log");
        BufferedWriter bw = new BufferedWriter(new FileWriter(fout, true));
        bw.write("output-> ");
        for (Map.Entry<String, CountryMap> entry : rankings.entrySet()) {
            bw.write("[" + entry.getKey() + ", " + entry.getValue().getOrderedMap() + "], ");
        }
        bw.newLine();
        System.out.println("File succesfully written: " + fout.getAbsolutePath());
        bw.close();
    }

}
