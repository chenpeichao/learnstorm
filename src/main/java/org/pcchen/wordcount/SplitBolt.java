package org.pcchen.wordcount;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * 接收spout传送的数据，并且将数据分割传送给下一级bolt
 * Created by ceek on 2017-02-27 12:27.
 */
public class SplitBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        tuple.getValueByField("line");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
