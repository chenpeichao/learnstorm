package org.pcchen.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 读取文件每行的数据，并且发送给下一级的bolt
 * Created by ceek on 2017-02-27 11:57.
 */
public class LocalFileSpout extends BaseRichSpout{
    private SpoutOutputCollector spoutOutputCollector;
    private BufferedReader br;
//    private List<Object> wordList = new ArrayList<Object>();

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        InputStreamReader inputStreamReader;
        try {
            this.spoutOutputCollector = spoutOutputCollector;
//            inputStreamReader = new InputStreamReader(new FileInputStream(new File("F:\\change\\data\\storm\\wordcount\\1.log")));
            inputStreamReader = new InputStreamReader(new FileInputStream(new File("/root/test/storm/wordcount/1.log")));
            br = new BufferedReader(inputStreamReader);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void nextTuple() {
        try {
            String line = br.readLine();
            if(StringUtils.isNotBlank(line)) {
                List<Object> wordList = new ArrayList<Object>();
                wordList.add(line);

                spoutOutputCollector.emit(wordList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }
}
