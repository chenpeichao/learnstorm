package org.pcchen.wordcount.ack_basebasicbolt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * storm中自实现ack机制测试：spout类
 * Created by ceek on  2017-03-06 15:24
 */
public class AckBaseBasicBoltSpout extends BaseRichSpout{
    private SpoutOutputCollector spoutOutputCollector;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        String uuid = UUID.randomUUID().toString().replace("_", "");
        spoutOutputCollector.emit(new Values(uuid), new Values(uuid));
        try {
            Thread.currentThread().sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("uuid"));
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("消息处理成功！");
        System.out.println(msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println(msgId);
        System.out.println("消息处理失败！");
        spoutOutputCollector.emit((List)msgId, msgId);
    }
}
