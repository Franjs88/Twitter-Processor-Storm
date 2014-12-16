package master.storm.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author fran
 */
public class Rankings {

    // Contains the top 3 topics by country
    private final HashMap<String, CountryMap> rankings;

    public Rankings() {
        this.rankings = new HashMap<String, CountryMap>();
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
	FileOutputStream fo = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fo));
        for (Map.Entry<String, CountryMap> entry : rankings.entrySet()) {
            bw.write("["+entry.getKey()+", "+entry.getValue().getOrderedMap().toString()+"]");
            bw.newLine();
        }
        bw.close();
    }

}
