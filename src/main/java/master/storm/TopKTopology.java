/*
 * Esta clase recibe la IP y el puerto donde se encuentra la Twitter App, y 
 * como tercer parametro recibe la lista con los paises de los cuales se quiera
 * evaluar el ranking (parametro de tipo String en el formado pais1,pais2,pais3
 * ejemplo: es,en,it,pt).
 * Por último, lanza la topología.
 */
package master.storm;

import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import master.storm.bolt.TweetCountBolt;
import master.storm.spout.TwitterSpout;
import master.storm.util.StormRunner;
import org.apache.log4j.Logger;

/**
 * La topología debe generar un resultado de las 3 palabras más repetidas en cada
 * uno de los paises de origen de los tweets.
 * @author fran
 */
public class TopKTopology {

    private static final Logger LOG = Logger.getLogger(TopKTopology.class);
    private static final int DEFAULT_RUNTIME_IN_SECONDS = 120;

    private final TopologyBuilder builder;
    private final String topologyName;
    private final Config topologyConfig;
    private final int runtimeInSeconds;

    public TopKTopology(String topologyName, String serverIP, Integer port,
            String countries) throws InterruptedException {
        builder = new TopologyBuilder();
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfiguration();
        runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

        wireTopology(serverIP, port, countries);
    }

    private static Config createTopologyConfiguration() {
        Config conf = new Config();
        conf.setDebug(true);
        return conf;
    }

    private void wireTopology(String serverIP, Integer port, String countries)
            throws InterruptedException {
        String spoutId = "twitterConsumer";
        String counterId = "counter";

        builder.setSpout(spoutId, new TwitterSpout(serverIP, port, countries), 1);
        // 600 segundos emite y escribe fichero
        builder.setBolt(counterId, new TweetCountBolt(100), 1).globalGrouping(spoutId);
    }

    public void runLocally() throws InterruptedException {
        StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
    }

    public void runRemotely() throws Exception {
        StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
    }

    //Args: 1) TopologyName 2)IP Address of TwitterApp 3)Server port 4)CountriesList
    public static void main(String[] args) throws Exception {
        String topologyName = "topKTopology";
        String serverAddress = args[1];
        int port = Integer.parseInt(args[2]);
        String countriesList = args[3];
        
        if (args.length >= 1) {
            topologyName = args[0];
        }
        boolean runLocally = true;
        if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
            runLocally = false;
        }
        LOG.info("Topology name: " + topologyName);
        LOG.info("Server IP address: " + serverAddress);
        LOG.info("Server port number: " + port);
        LOG.info("List of Countries: " + countriesList);

        // We create the topology with the parameters given by console
        TopKTopology tkt = new TopKTopology(topologyName, serverAddress, port, countriesList);
        if (runLocally) {
            LOG.info("Running in local mode");
            tkt.runLocally();
        } else {
            LOG.info("Running in remote (cluster) mode");
            tkt.runRemotely();
        }
    }

}
