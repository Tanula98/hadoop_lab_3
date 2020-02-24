import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;


public class Main {

    private final static int CODE = 0;
    private final static int DESCRIPTION = 1;

    private final static int ORIGIN_AIRPORT_ID = 11;
    private final static int DEST_AIRPORT_ID = 14;
    private final static int ARR_DELAY_NEW = 18;
    private final static int CANCELLED = 19;


    public static void main(String[] args) {
        //а. Инициализируем Spark
        SparkConf conf = new SparkConf().setAppName("lab3");
        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("Spark init");

        //б. Загружаем исходные наборы данных в RDD с помощью метода
        JavaRDD<String> flightFile = sc.textFile("hdfs://localhost:9000/user/tatiana/664600583_T_ONTIME_sample.csv");
        JavaRDD<String> airportFile = sc.textFile("hdfs://localhost:9000/user/tatiana/L_AIRPORT_ID.csv");

        JavaRDD<String> splittedFlightFile = flightFile.flatMap(
                s -> Arrays.stream(s.split("\n")).iterator()
        );
        JavaRDD<String> splittedAirportFile = airportFile.flatMap(
                s -> Arrays.stream(s.split("\n")).iterator()
        );

        //в. Преобразуем RDD в RDD пару ключ значение с помощью метода
        //mapToPair
        String firstLineAirport = airportFile.first();
        Map<String, String>  stringAirportDataMap = airportFile.filter(
                s -> !s.equals(firstLineAirport)
        ).mapToPair(
                s -> {
                    String[] splittedLine = s.replaceAll("\"", "").split(",");
                    return new Tuple2<>(splittedLine[CODE], splittedLine[DESCRIPTION]);
                }
                //е. для связывания с таблицей аэропортов — предварительно
                //выкачиваем список аэропортов в главную функцию с помощью метода
                //collectAsMap
        ).collectAsMap();


        String firstLineFligh = splittedFlightFile.first();
        JavaRDD<FlightData> flightDataJavaRDD = splittedFlightFile.filter(
                s -> !s.equals(firstLineFligh)
        ).map(
                s -> {
                    String[] splittedLine = s.replaceAll("\"", "").split(",");
                    String originAirportId = splittedLine[ORIGIN_AIRPORT_ID];
                    String destAirportId = splittedLine[DEST_AIRPORT_ID];
                    Boolean cancelled = Double.parseDouble(splittedLine[CANCELLED]) == 1.0;
                    Double arrDelayNew;
                    if (!splittedLine[ARR_DELAY_NEW].equals("")){
                        arrDelayNew = !cancelled ? Double.parseDouble(splittedLine[ARR_DELAY_NEW]) : -1d;
                    } else {
                        arrDelayNew = -1d;
                    }
                    return new FlightData(originAirportId, destAirportId, arrDelayNew, cancelled);
                }
        );
        JavaPairRDD<Tuple2<String, String>, FlightData> flightPair = flightDataJavaRDD.mapToPair(
                s -> new Tuple2<>(new Tuple2<>(s.getOriginAirportId(), s.getDestAirportId()), s)
        );

        JavaPairRDD<Tuple2<String, String>, Statistic> statisticPair = flightPair.combineByKey(
                s -> new Statistic(s.getArrDelayNew(), s.getCancelled(), 1l),
                (a, b) -> Statistic.addValue(a, b.getArrDelayNew(), b.getCancelled()),
                Statistic::add
        );



        //ё. создаем в основном методе main переменную broadcast
        final Broadcast<Map<String, String>> airportsBroadcasted =
                sc.broadcast(stringAirportDataMap);


        //ж. в методе map преобразуем итоговый RDD содержащий статистические
        //данные — обогащаем его именами аэропортов, обращаясь внутри
        //функций к объекту airportsBroadcasted.value()
        JavaRDD<Statistic> result = statisticPair.map(
                s -> new Statistic(s, airportsBroadcasted.value())
        );
        result.saveAsTextFile("output3");
    }

}
