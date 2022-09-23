package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

public class HubIdentifier extends BaseBasicBolt
{

    //create "airportArray" array of type "Airport" class
    Airport[] airportArray = new Airport[40];

    //Hashmap to read the flight code information
    HashMap<String,String> flightCodeInfo = new HashMap<String,String>();

    private FileReader fileReader;

    public void cleanup()
    {}

    public void prepare(Map conf, TopologyContext context) {

        // read the airportArray data by calling "readAirportData"
        try {
            fileReader = new FileReader(conf.get("AirportsData").toString());
            readAirportData();

            this.fileReader = new FileReader((String) conf.get("FlightCodeData"));
            readFlightCodeInfo();
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+conf.get("AirportsData")+"]");
        }
    }

    // Open a “AirportsData” file and read data on 40 airportArray.
    public void readAirportData() {

        String str;
        int i = 0;

        BufferedReader reader = new BufferedReader(fileReader);

        try {
            //Read all lines
            while((str = reader.readLine()) != null && i < airportArray.length)
            {
                //if the string is not empty
                if(!str.isEmpty())
                {
                    //split the string and add data into "airportArray" array
                    String[] details = str.split(",");
                    airportArray[i] = new Airport();
                    airportArray[i].city = details[0];
                    airportArray[i].code = details[1];
                    airportArray[i].latitude = details[2];
                    airportArray[i].longitude = details[3];
                    i++;
                }
            }
        }
        catch(Exception e) {
            throw new RuntimeException("Error reading file",e);
        }
    }


    // Open a “FlightCodeData” file and read data on flightCodeInfo hashmap.
    public void readFlightCodeInfo() {

        try {
        BufferedReader reader1 = new BufferedReader(fileReader);

        String str1;

        //Read all lines from AirlineCode.txt
        while ((str1 = reader1.readLine()) != null) {

            if (!str1.trim().isEmpty()) {

                if(str1!=":"){
                    String[] flightCodeInfo = str1.split(":");

                    if(flightCodeInfo[0] != null && flightCodeInfo[0] != "n/a"
                            && flightCodeInfo[0].length() != 0
                            && flightCodeInfo[1] != null && flightCodeInfo[1] != "n/a"
                            && flightCodeInfo[1].length() != 0) {

                        this.flightCodeInfo.put(flightCodeInfo[0],flightCodeInfo[1]);
                    }
                }
            }
        }
        } catch(Exception e) {
            throw new RuntimeException("Error reading file",e);
        }
    }


    /**
     * The bolt will receive the flight call-sign, latitude, longtute
     * and find the flights which are within 20miles to the airport
     *
     * The hubIdentifier will find the difference between the flight and
     * airport latitude and longitude and emit the values
     * if it within 20 miles
     */
    public void execute(Tuple tuple, BasicOutputCollector collector)
    {
        //get the flight call-sign, flight latitude and longitude
        String flight_call_sign = tuple.getStringByField("call sign").trim();
        String flight_latitude = tuple.getStringByField("latitude");
        String flight_longitude = tuple.getStringByField("longitude");

        for(int i = 0; i < airportArray.length; i++)
        {
            Double diffLat = Math.abs(Double.parseDouble(flight_latitude) -
                    Double.parseDouble(airportArray[i].latitude)) * 70;

            Double diffLong = Math.abs(Double.parseDouble(flight_longitude) -
                    Double.parseDouble(airportArray[i].longitude)) * 45;

            //if flight is range, emit the details
            if ((diffLat  <= 20) && (diffLong  <= 20))
            {
                //if flight call sign is of length '3'
                if ((flight_call_sign.length() > 2) && (flight_call_sign!=null) && (flight_call_sign!="null")) {

                    String code = flight_call_sign.substring(0,3);
                    String fullForm;
                    if (flightCodeInfo.get(code) == null || flightCodeInfo.get(code).length() == 0)
                        fullForm = "n/a";
                    else
                        fullForm = flightCodeInfo.get(code);
//
                    String codeAndFullform = code + "  " + "(" + fullForm + ")";
                    collector.emit(new Values(airportArray[i].city, airportArray[i].code, codeAndFullform));
                }
            }
        }
    }

    /**
     * The bolt will only emit the field "airportArray.city", "airportArray.code", "call-sign"
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("airport.city", "airport.code", "call-sign"));
    }
}
