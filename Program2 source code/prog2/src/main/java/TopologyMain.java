import spouts.FlightsDataReader;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.AirlineSorter;
import bolts.HubIdentifier;


public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
         
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("flightsData-reader",new FlightsDataReader(), 2);
		builder.setBolt("hub-identifier", new HubIdentifier(),1)
			.shuffleGrouping("flightsData-reader");
		builder.setBolt("airline-sorter", new AirlineSorter(),1)
			.fieldsGrouping("hub-identifier", new Fields("airport.city"));

		System.err.println("Number of threads in FlightDataReader " + "2" );
		System.err.println("Number of threads in HubIdentifier " + "1");
		System.err.println("Number of threads in AirlineSorter " + "1" );
		
        //Configuration
		Config conf = new Config();
		conf.put("FlightsFile", args[0]);
		conf.put("AirportsData", args[1]);
		conf.put("FlightCodeData", args[2]);
		conf.setDebug(false);
        //Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
		Thread.sleep(5000);
		cluster.shutdown();
	}
}
