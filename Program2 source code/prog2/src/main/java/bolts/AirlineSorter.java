package bolts;

import java.util.*;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class AirlineSorter extends BaseBasicBolt {

    Integer id;
    String name;
    Map<String, Map<String, Integer>> stats;
    long begin,end;

    /**
     * At the end of the spout (when the cluster is shutdown
     * We will show the flight counts
     */
    @Override
    public void cleanup() {

        System.out.println("-- Flight Counter [" + name + "-" + id + "] --");

        //sort the flight count per airport
        for (Map.Entry<String, Map<String, Integer>> en : stats.entrySet()) {
            Map<String, Integer> sortedAirlines = sortByValue(en.getValue());
            stats.put(en.getKey(),sortedAirlines);
        }

        for (Map.Entry<String, Map<String, Integer>> airport : stats.entrySet()) {
            int totalFlights = 0;
            System.out.println("At Airport: " + airport.getKey() );
            for (Map.Entry<String, Integer> airline : airport.getValue().entrySet()) {
                System.out.println("            " + airline.getKey() + ": " + airline.getValue());
                totalFlights += airline.getValue();
            }
            System.out.println("            " + "total #flights = " + totalFlights );
            System.out.println();

        }

        end = System.currentTimeMillis();
        long time = end - begin;
        System.err.println("Total elapsed time in " + time + " milliSeconds");
    }

    //Method to sort the hashmap on the basis of value
    public static Map<String, Integer> sortByValue(Map<String, Integer> hm)
    {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer> > list =
                new LinkedList<Map.Entry<String, Integer> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2)
            {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }


    /**
     * On create
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        begin = System.currentTimeMillis();

        this.stats = new HashMap<String, Map<String, Integer>>();
        this.name = context.getThisComponentId();
        this.id = context.getThisTaskId();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        String airportCity = input.getStringByField("airport.city");
        String airportCode = input.getStringByField("airport.code");
        String callSign = input.getStringByField("call-sign");

        String airportCodeWithCity = airportCode + "(" + airportCity + ")";

        /**
         * If the "airportCodeWithCity" dosn't exist in the map we will create
         * this, if not We will add 1
         */
        if(!stats.containsKey(airportCodeWithCity)){
            Map<String, Integer> airLine = new HashMap<String, Integer>();
            airLine.put(callSign,1);
            stats.put(airportCodeWithCity,airLine);
        }
        else {
            Map<String, Integer> airLine = stats.get(airportCodeWithCity);
            if(airLine.containsKey(callSign)){

                int airLineCount = airLine.get(callSign);
                airLine.put(callSign,airLineCount+1);

            }else{
                airLine.put(callSign,1);
            }
        }
    }
}
