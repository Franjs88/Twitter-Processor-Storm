package master.storm.bolt;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.io.IOException;
import java.util.Map;
import java.util.logging.Level;
import master.storm.tools.Rankings;
import master.storm.util.TupleHelpers;
import org.apache.log4j.Logger;

/**
 * Esta clase realiza el conteo de tweets que le llegan del WordSpout basado en
 * un modelo de ventana deslizante.
 *
 * @author fran
 */
public class TweetCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = 5537727428628598519L;
    private static final Logger LOG = Logger.getLogger(TweetCountBolt.class);
    private OutputCollector collector;
    // Contains a list of rankings by each country
    private Rankings topKRankings;
    // The seconds that must pass until the bolt receive a tick tuple
    private final int emitFrequencyInSeconds;
    private long limiteSuperior;
    private long limiteActual;

    public TweetCountBolt(int emitFrequencyInSeconds) {
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("country", "top1", "top2", "top3"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        topKRankings = new Rankings();
        limiteSuperior = 0;
        limiteActual = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        // After 10 minutes we write to file
        if(limiteSuperior == 0) {
            limiteSuperior = (long) tuple.getValueByField("timestamp")+30;
            System.out.println("Limite superior es: "+limiteSuperior);
        } else {
            limiteActual = (long) tuple.getValueByField("timestamp");
            System.out.println("Limite actual es: "+limiteActual);
        }
        System.out.println("Tiempo que queda: "+(limiteSuperior-limiteActual));
        if (limiteSuperior <= limiteActual) {
            System.out.println("Han pasado 10 min, escribiendo a fichero...");
            limiteSuperior = 0;
            writeTopKToFile();
        } else {
            // We continue counting topics
            countObjAndAck(tuple);
            System.out.println("INCREMENTAMOS RANKING");
        }
    }

    private void writeTopKToFile() {
        try {
            topKRankings.writeOrdered();
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(TweetCountBolt.class.getName())
                    .log(Level.SEVERE, "Error al escribir fichero", ex);
        }
    }

    private void countObjAndAck(Tuple tuple) {
        String country = (String) tuple.getValue(1);
        String topic = ((String) tuple.getValue(2));
        topKRankings.incrementCount(country, topic);
        collector.ack(tuple);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        //Map<String, Object> conf = new HashMap<String, Object>();
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, emitFrequencyInSeconds);
        return conf;
    }
}
