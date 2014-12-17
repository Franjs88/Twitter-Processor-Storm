package master.storm.spout;

import backtype.storm.tuple.Values;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
    public ArrayList<Values> readTweet() {
        ArrayList<Values> values = null;
        try {
            String in;
            //Leemos
            if ((in = reader.readLine()) != null) {
                JSONObject tweet = (JSONObject) jsonParser.parse(in);
                values = parseTweet(tweet);
                System.out.println("ReadTweet: Values contiene: "+values);
            }
        } catch (IOException | ParseException ex) {
            Logger.getLogger(TwitterAppClient.class.getName()).log(Level.SEVERE, null, ex);
        }
        return values;
    }

    /*
     * Inserts in the queue:"cola", one hashtag for each word with # of a
     * same user tweet.
     */
    private ArrayList<Values> parseTweet(JSONObject tweet) {
        ArrayList<Values> list = new ArrayList<>();
        JSONObject user = (JSONObject) tweet.get("user");
        String paisOrigen = (String) user.get("lang");
        // We only continue if the country is on the list
        if (isOnCountryList(paisOrigen)) {
            System.out.println("Esta en la lista de paises");
            // If it is on country list, we get the username
            String usuario = (String) user.get("screen_name");

            // We convert to timestamp the tweet
            long timestamp = Long.parseLong((String) tweet.get("timestamp_ms"));
            timestamp = (long) (timestamp * 0.001);

            // We filter hashtags.
            // Generates a tuple for each hashtag in the tweet of the same user
            JSONObject entities = (JSONObject) tweet.get("entities");
            if (entities.containsKey("hashtags")) {
                JSONArray hashtags = (JSONArray) entities.get("hashtags");
                // Parse and insert in the queue
                list = parseHashtags(hashtags, usuario, paisOrigen, timestamp);
            }

        } else {
            System.out.println("Pais no esta en la lista. Filtrado");
        }
        System.out.println("La lista de valores a devolver es: "+list.toString());
        return list;
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

    private ArrayList<Values> parseHashtags(JSONArray hashtags, String user, String pais,
            long timestamp) {
        Iterator iter = hashtags.iterator();
        ArrayList<Values> list = new ArrayList<>();
        JSONObject jsonHashtag;
        String palabra;
        while (iter.hasNext()) {
            jsonHashtag = (JSONObject) iter.next();
            palabra = (String) jsonHashtag.get("text");
            System.out.println("####Va a meter EN COLA: "
                    + user + ", " + pais + ", " + palabra + ", " + timestamp);

            list.add(new Values(user, pais, palabra, timestamp));

            System.out.println("####Metido en lista la palabra" + palabra);

        }
        return list;
    }

}
