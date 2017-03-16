package org.pcchen.wordcount.kafkaredis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * 将kafka传递过来的数据(字符串)，进行切割
 * Created by ceek on 2017-03-15 23:21.
 */
public class KafkaSplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        List<Object> strList = new ArrayList<Object>();

        //1、数据如何获取
        byte[] juzi = (byte[])tuple.getValueByField("bytes");
        //2、进行切割
        String[] strings = new String(juzi).split(" ");
        for(String value : strings) {
//            strList.add(value);
            outputCollector.emit(new Values(value));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("splitLine"));
    }
}
