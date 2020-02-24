import scala.Tuple2;

import java.io.Serializable;
import java.util.Map;

public class Statistic implements Serializable  {

    private Double maxArrDelayNew;
    private Long lateAndCancelled;
    private Long sum;
    private String originName;
    private String destName;

    public Statistic(Double maxArrDelayNew, Long lateAndCancelled, Long sum){
        this.lateAndCancelled = lateAndCancelled;
        this.maxArrDelayNew = maxArrDelayNew;
        this.sum = sum;
    }
    public Statistic(Tuple2<Tuple2<String, String>, Statistic> s, Map<String, String> dictionary){
        this.maxArrDelayNew = s._2.maxArrDelayNew;
        this.lateAndCancelled = s._2.lateAndCancelled;
        this.sum = s._2.sum;
        this.originName = dictionary.get(s._1._1);
        this.destName = dictionary.get(s._1._2);
    }

    public static Statistic addValue(Statistic a, Double maxArrDelayNew, Long lateAndCancelled){
        return new Statistic(Double.max(a.maxArrDelayNew, maxArrDelayNew), a.lateAndCancelled+lateAndCancelled, a.sum+1l);
    }
    public static Statistic add(Statistic a, Statistic b){
        return new Statistic(Double.max(a.maxArrDelayNew, b.maxArrDelayNew), a.lateAndCancelled+b.lateAndCancelled, a.sum+b.sum);
    }
    public Double getMaxArrDelayNew() {
        return maxArrDelayNew;
    }

    public Long getLateAndCancelled() {
        return lateAndCancelled;
    }

    public Long getSum() {
        return sum;
    }
    public String toString(){
        return "From " + originName + " to " + destName
                + ". Max delay: " + maxArrDelayNew
                + " Late and cancelled: " + String.format("%.2f", ((double)lateAndCancelled/sum)*100.0);
    }
}
