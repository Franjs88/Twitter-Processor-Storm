package master.storm.spout;

import backtype.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author fran
 */
public class TwitterAppClient {

    private final String IPAdd;
    private final Integer port;
    private final String countries;
    private Socket socket;
    private BufferedReader reader;
    private JSONParser jsonParser;

    public TwitterAppClient(String IP, Integer port, String countries) {
        this.IPAdd = IP;
        this.port = port;
        this.jsonParser = new JSONParser();
        this.countries = countries;
    }

    public void connect() {
        try {
            this.socket = new Socket(IPAdd, port);
        } catch (IOException ex) {
            Logger.getLogger(TwitterAppClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        InputStreamReader inputStream = null;
        try {
            inputStream = new InputStreamReader(socket.getInputStream());
        } catch (IOException ex) {
            Logger.getLogger(TwitterAppClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        this.reader = new BufferedReader(inputStream);
    }

    public void disconnect() {
        try {
            socket.close();
        } catch (IOException ex) {
            Logger.getLogger(TwitterAppClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    // Insert in the queue that nextTuple reads
    public boolean readTweet(LinkedBlockingQueue<Values> cola) {
        boolean isNull = true;
        try {
            String in;
            //Leemos mientras lleguen tweets
            if ((in = reader.readLine()) != null) {
                JSONObject tweet = (JSONObject) jsonParser.parse(in);
                List<Values> values = parseTweet(tweet);
                // If values is empty, we return false
                if (values != null) {
                    for (Values v : values) {
                        System.out.println("Insertando el cola: " + v.toString());
                        cola.add(v);
                    }
                    isNull = false;
                }
            }
        } catch (IOException | ParseException ex) {
            Logger.getLogger(TwitterAppClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        return isNull;
    }

    /*
     * Inserts in the queue:"cola", one hashtag for each word with # of a
     * same user tweet.
     */
    private List<Values> parseTweet(JSONObject tweet) {
        List<Values> ret = new ArrayList<>();
        JSONObject place = (JSONObject) tweet.get("place");
        System.out.println(
                "###########Parseando Place... ");
        if (place != null) {
            String paisOrigen = (String) place.get("country_code");
            System.out.println(
                    "###########Parseando_PaisOrigen "
                    + paisOrigen);
            // We only continue if the country is on the list
            if (isOnCountryList(paisOrigen)) {
                System.out.println(
                        "#############################Pais no es NUll... ");
                JSONObject user = (JSONObject) tweet.get("user");
                String usuario = (String) user.get("screen_name");

                long timestamp = convertToTimeStamp(tweet);

                // We filter hashtags.
                // Generates a tuple for each hashtag in the tweet of the same user
                JSONObject entities = (JSONObject) user.get("entities");
                JSONArray hashtags = (JSONArray) entities.get("hashtags");

                System.out.println(
                        "#############################Parseando hashtags... "
                        + paisOrigen + usuario + timestamp + hashtags);

                Iterator iter = hashtags.iterator();
                while (iter.hasNext()) {
                    String palabra = (String) iter.next();
                    System.out.println("####Va a meter EN COLA: "
                            + usuario + ", " + paisOrigen + ", " + palabra + ", " + timestamp);
                    ret.add(new Values(usuario, paisOrigen, palabra, timestamp));
                    System.out.println("####Metido en cola la palabra" + palabra);
                }
            } else {
                System.out.println("Pais no esta en la lista. Filtrado");
            }
        } else {
            System.out.println("Place is null. Filtrado");
        }
        return ret;
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
            System.out.println("isOnCountryList?: " + next);
            System.out.println("isEquals: " + next.equals(paisOrigen));
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

}
