package master.storm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import org.scribe.builder.ServiceBuilder;
import org.scribe.builder.api.TwitterApi;
import org.scribe.model.OAuthRequest;
import org.scribe.model.Response;
import org.scribe.model.Token;
import org.scribe.model.Verb;
import org.scribe.oauth.OAuthService;

/**
 * This is the Twitter Streaming API Reader Class
 *
 * @author Fco. Javier Sánchez Carmona
 */
public class TwitterApp {

    String STREAMING_API_URL_FILTER = "https://stream.twitter.com/1.1/statuses/filter.json";
    OAuthService service;
    Token accessToken;
    BufferedReader reader;

    public TwitterApp(String userKey, String userSecret, String token, String tokenSecret) {
        service = new ServiceBuilder()
                .provider(TwitterApi.class)
                .apiKey(userKey)
                .apiSecret(userSecret)
                .build();
        accessToken = new Token(token, tokenSecret);
    }

    public void connect(String csvFilter) {
        OAuthRequest request = new OAuthRequest(Verb.POST, STREAMING_API_URL_FILTER);
        request.addHeader("version", "HTTP/1.1");
        request.addHeader("host", "stream.twitter.com");
        request.setConnectionKeepAlive(true);
        request.addHeader("user-agent", "Twitter Stream Reader");
        request.addBodyParameter("track", csvFilter); //Set keywords you'd like to track here
        service.signRequest(accessToken, request);
        Response response = request.send();

        //Create a reader to read Twitter's stream
        reader = new BufferedReader(new InputStreamReader(response.getStream()));
    }

    public void disconnect() throws IOException {
        reader.close();
    }

    public String getTweet() throws IOException, ParseException {
        String line = reader.readLine();

        while (line == null || line.length() <= 0) {
            line = reader.readLine();
        }

        return line;
    }

    public static void main(String[] args) throws IOException, ParseException {
        ServerSocket server = new ServerSocket(9000);
        TwitterApp tr = new TwitterApp(
                "vB9NlAPYokHbZmeObPsH1X9Zu",
                "lvKI0TOX4fhn0sx3FYlPeQn9fw7ivzTdcj3pZYxnRYfnhdauXY",
                "231776169-P3JdpJtsOckb3Ts0f35sKh4i0DavYYSj5clfJIGb",
                "9YBu2QKGasYbkBdQzOv4CVWCiry0O7XqZigHIUyyB6upb");
        tr.connect("madrid");
        String tweet;
        /////////////////////////////////

        //We start the server
        try {
            while (true) {
                System.out.println("Magic happens on port: " + server.getLocalPort());
                //if a connection is established, we serve the tweets
                try (Socket socket = server.accept()) {
                    tweet = tr.getTweet();
                    System.out.println("Nueva conexión recibida");
                    PrintWriter out
                            = new PrintWriter(socket.getOutputStream(), true);
                    out.println(tweet);
                }
            }

        } catch (IOException | ParseException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Cerrando Socket");
            server.close();
            tr.disconnect();
        }
    }
}
