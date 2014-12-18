package master.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 *
 * @author fran
 */
public class TotalRankBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("pais","top1","top2","top3"));
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        collector.emit(new Values(input));
    }

    
}
