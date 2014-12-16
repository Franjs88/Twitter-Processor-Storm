package master.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import static backtype.storm.utils.Time.LOG;
import backtype.storm.utils.Utils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import master.storm.tools.Hashtag;
import org.json.simple.JSONArray;

/**
 * Spout que abre socket con la TwitterApp y almacena en una cola los tweets que
 * le llegan y los emite al TweetCountBolt.
 *
 * @author fran
 */
public class TwitterSpout extends BaseRichSpout {

    private Integer port = null;
    private String serverAddress = null;
    private String countries = null; //"pais1,pais2,pais3"

    //Not class atributes
    private LinkedBlockingQueue<Hashtag> cola = null;
    private SpoutOutputCollector _collector;
    private JSONParser jsonParser;

    public TwitterSpout(String serverIP, Integer port, String countries) {
        this.port = port;
        this.serverAddress = serverIP;
        this.countries = countries;
    }

    @Override
    public void open(Map map, TopologyContext tc, SpoutOutputCollector collector) {
        jsonParser = new JSONParser();
        cola = new LinkedBlockingQueue<Hashtag>(1000);
        _collector = collector;

        try {
            Socket s = new Socket(serverAddress, port);
            InputStreamReader inputStream = new InputStreamReader(s.getInputStream());
            BufferedReader reader = new BufferedReader(inputStream);
            String in;

            //Leemos mientras lleguen tweets
            while ((in = reader.readLine()) != null) {
                try {
                    JSONObject tweet = (JSONObject) jsonParser.parse(in);
                    parseTweet(tweet);

                } catch (ParseException e) {
                    LOG.error("Error parsing message from twitter", e);
                }
            }
        } catch (IOException ex) {
            Logger.getLogger(TwitterSpout.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public void nextTuple() {
        Hashtag ret = cola.poll();
        if (ret == null) {
            Utils.sleep(50);
        } else {
            _collector.emit(new Values(ret));
        }
    }

    /*
     * Inserts in the queue:"cola", one hashtag for each word with # of a
     * same user tweet.
     */
    private void parseTweet(JSONObject tweet) {
        JSONObject place = (JSONObject) tweet.get("place");
        
        if (place != null) {
            String paisOrigen = (String) place.get("country_code");

            // We only continue if the country is on the list
            if (isOnCountryList(paisOrigen)) {
                JSONObject user = (JSONObject) tweet.get("user");
                String usuario = (String) user.get("screen_name");

                long timestamp = convertToTimeStamp(tweet);

            // We filter hashtags.
                // Generates a tuple for each hashtag in the tweet of the same user
                JSONObject entities = (JSONObject) user.get("entities");
                JSONArray hashtags = (JSONArray) entities.get("hashtags");

                Iterator iter = hashtags.iterator();
                while (iter.hasNext()) {
                    String palabra = (String) iter.next();
                    try {
                        cola.put(new Hashtag(usuario, paisOrigen, palabra, timestamp));
                        System.out.println("####METIDO EN COLA: "
                                +usuario+", "+paisOrigen+", "+palabra+", "+timestamp);
                    } catch (InterruptedException ex) {
                        Logger.getLogger(TwitterSpout.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            } else {
                System.out.println("Pais no esta en la lista. Filtrado");
            }
        }
        else {
            System.out.println("Place is null. Filtrado");
        }

    }

    /*
     * @return true if paisOrigen is on the country_codes list.
     * @return false if not
     */
    private boolean isOnCountryList(String paisOrigen) {
        List<String> countryCodes = Arrays.asList(this.countries.split(","));
        Iterator iter = countryCodes.iterator();
        boolean result = false;

        while (iter.hasNext()) {
            String next = (String) iter.next();
            if (next.equals(paisOrigen)) {
                result = true;
            }
        }
        return result;
    }

    private long convertToTimeStamp(JSONObject tweet) {
        String createdAt = (String) tweet.get("created_at");
        SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd kk:mm:ss z yyyy");

        Date date = null;
        try {
            date = formatter.parse(createdAt);
        } catch (java.text.ParseException ex) {
            Logger.getLogger(TwitterSpout.class.getName()).log(Level.SEVERE, null, ex);
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        long time = calendar.getTimeInMillis();
        long current = System.currentTimeMillis();
        long diff = current - time; //Time difference in milliseconds
        return diff / 1000;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("hashtag"));
    }

}
