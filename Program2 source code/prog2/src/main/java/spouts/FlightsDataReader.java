package spouts;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

public class FlightsDataReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    public void ack(Object msgId) {
        System.out.println("OK:"+msgId);
    }
    public void close() {}
    public void fail(Object msgId) {
        System.out.println("FAIL:"+msgId);
    }


    /**
     * The only thing that the methods will do It is emit each
     * flight detail
     */
    public void nextTuple() {
        /**
         * The nextuple it is called forever, so if we have been readed the file
         * we will wait and then return
         */
        if (completed) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                //Do nothing
            }
            return;
        }
        try{

            //call the dataNormalizer to get flight data
            ArrayList<ArrayList<String>> listOfFlights = dataNormalizer();

            //Iterate on the list and emit the flight data
            for(int i = 0; i < listOfFlights.size(); i++) {

                ArrayList<String> flightIterator = listOfFlights.get(i);
                this.collector.emit(new Values(
                        flightIterator.get(0),
                        flightIterator.get(1),
                        flightIterator.get(2),
                        flightIterator.get(3),
                        flightIterator.get(4),
                        flightIterator.get(5),
                        flightIterator.get(6),
                        flightIterator.get(7),
                        flightIterator.get(8),
                        flightIterator.get(9),
                        flightIterator.get(10),
                        flightIterator.get(11),
                        flightIterator.get(12),
                        flightIterator.get(13),
                        flightIterator.get(14),
                        flightIterator.get(15),
                        flightIterator.get(16)));

            }
        }catch(Exception e){
            throw new RuntimeException("Error reading tuple",e);
        }finally{
            completed = true;
        }
    }

        /**
     * It is a utility fuction which will normalize data per flight
     */
    public ArrayList<ArrayList<String>> dataNormalizer() {

        ArrayList<ArrayList<String>> listOfFlights = new ArrayList<ArrayList<String>>();

        try {
            // parsing file through JSONParser
            Object obj = new JSONParser().parse(fileReader);

            // typecasting obj to JSONObject
            JSONObject jo = (JSONObject) obj;

            // getting states
            JSONArray statesArray = (JSONArray) jo.get("states");

            //get the each array of flights from "states"
            // and add it to "listOfFlights"
            for (int i = 0; i < statesArray.size(); i++) {

                JSONArray jArray = (JSONArray) statesArray.get(i);

                //put "0" if the any value is null
                for (int j = 0; j < jArray.size(); j++) {
                    if (jArray.get(j) == null) {
                        jArray.set(j, "0");
                    }
                }

                //creating a arrayList called "flightIterator" and add data into it
                ArrayList<String> flightIterator = new ArrayList<String>();
                for (int j = 0; j < jArray.size(); j++) {
                    flightIterator.add(jArray.get(j).toString());
                }

                //add each flifgt data into a list
                listOfFlights.add(flightIterator);
            }

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //return the "listOfFlights"
        return listOfFlights;

    }

    /**
     * We will create the file and get the collector object
     */
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        try {
            this.fileReader = new FileReader(conf.get("FlightsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+conf.get("FlightsFile")+"]");
        }
        this.collector = collector;
    }

    /**
     * Declare the output field "word"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("transponder address","call sign","origin country"
                ,"last timestamp1","last timestamp2","longitude","latitude","altitude (barometric)"
                ,"surface or air","velocity (meters/sec)", "degree north = 0",
        "vertical rate","sensors","altitude (geometric)","transponder code","special purpose","origin"));
    }
}
