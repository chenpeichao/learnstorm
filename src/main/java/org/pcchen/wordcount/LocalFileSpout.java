package org.pcchen.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 读取文件每行的数据，并且发送给下一级的bolt
 * Created by ceek on 2017-02-27 11:57.
 */
public class LocalFileSpout extends BaseRichSpout{
    private SpoutOutputCollector spoutOutputCollector;
    private List<Object> wordList = new ArrayList<Object>();

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        InputStreamReader inputStreamReader;
        try {
            inputStreamReader = new InputStreamReader(new FileInputStream(new File("")));
            BufferedReader br = new BufferedReader(inputStreamReader);
            String line = br.readLine();

            wordList.add(line);
            this.spoutOutputCollector = spoutOutputCollector;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        spoutOutputCollector.emit(wordList);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
