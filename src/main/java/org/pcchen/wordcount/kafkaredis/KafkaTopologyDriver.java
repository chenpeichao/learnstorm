package org.pcchen.wordcount.kafkaredis;

import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.kafka.KafkaSpout;

/**
 * storm读取kafka中的数据的主题类topology
 * Created by ceek on 2017-03-15 23:22.
 */
public class KafkaTopologyDriver {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("kafkaSpout", new KafkaSpout(new SpoutConfig(new ZkHosts(""), "pc", "/storm", "")));
    }
}
