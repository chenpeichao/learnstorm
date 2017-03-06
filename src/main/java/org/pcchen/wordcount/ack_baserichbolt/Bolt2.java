package org.pcchen.wordcount.ack_baserichbolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import jdk.internal.util.xml.impl.Input;

import java.util.Map;

/**
 * 测试ack机制的第二个bolt
 * Created by ceek on 2017-03-06 0:57.
 */
public class Bolt2 extends BaseRichBolt {
    private OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String uuid = (String) tuple.getValue(0);
        try {
            int i = 1 / 0;
            outputCollector.emit(tuple, new Values(uuid));
            outputCollector.ack(tuple);
        } catch (Exception e){
            System.out.println(("除数不能为0"));
            System.out.println("bolt2的异常");
            outputCollector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("uuid"));
    }
}
