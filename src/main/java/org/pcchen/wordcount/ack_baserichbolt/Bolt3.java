package org.pcchen.wordcount.ack_baserichbolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import ch.qos.logback.core.net.SyslogOutputStream;

import java.util.Map;

/**
 * 测试ack机制的第三个bolt
 * Created by ceek on 2017-03-06 0:57.
 */
public class Bolt3 extends BaseRichBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String uuid = (String) tuple.getValue(0);
//        outputCollector.emit(new Values(uuid));
        outputCollector.ack(tuple);
        System.out.println("bolt3:" + uuid);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("uuid"));
    }
}
