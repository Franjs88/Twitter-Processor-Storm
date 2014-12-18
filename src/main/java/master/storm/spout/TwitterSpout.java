package master.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Spout que realiza peticiones a la TwitterApp y almacena en una cola los
 * tweets que le llegan y los emite al TweetCountBolt.
 *
 * @author fran
 */
public class TwitterSpout extends BaseRichSpout {

    private Integer port = null;
    private String serverAddress = null;
    private String countries = null; //"pais1,pais2,pais3"

    //Not class atributes
    private LinkedBlockingQueue<Values> cola = null;
    private SpoutOutputCollector _collector;
    TwitterAppClient client;

    public TwitterSpout(String serverIP, Integer port, String countries) {
        this.port = port;
        this.serverAddress = serverIP;
        this.countries = countries;
    }

    @Override
    public void open(Map map, TopologyContext tc, SpoutOutputCollector collector) {
        cola = new LinkedBlockingQueue<Values>();
        _collector = collector;
        client = new TwitterAppClient(serverAddress, port, countries);

    }

    @Override
    public void nextTuple() {
        ArrayList<Values> vList = new ArrayList<>();
        while (cola.isEmpty()) {
            client.connect();
            vList = client.readTweet();
            if (vList != null) {
                cola.addAll(vList);
            }
            client.disconnect();
        }
        // If not null, we emit the tuple
        Values ret = cola.poll();
        if (ret != null) {
            _collector.emit(ret);
            System.out.println("Emiting tweet to TweetCountBolt");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("usuario", "paisOrigen", "palabra", "timestamp"));
    }

}
