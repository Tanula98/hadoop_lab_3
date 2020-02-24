import java.io.Serializable;

public class FlightData implements Serializable {

    private String originAirportId;
    private String destAirportId;
    private Double arrDelayNew;
    private Boolean cancelled;

    public FlightData(String originAirportId, String destAirportId, Double arrDelayNew, Boolean cancelled){
        this.originAirportId = originAirportId;
        this.destAirportId = destAirportId;
        this.arrDelayNew = arrDelayNew;
        this.cancelled = cancelled;
    }

    public Long getCancelled() {
        return cancelled || arrDelayNew > 0 ? new Long(1) :new Long(0);

    }

    public Double getArrDelayNew() {
        return arrDelayNew;
    }

    public String getDestAirportId() {
        return destAirportId;
    }

    public String getOriginAirportId() {
        return originAirportId;
    }
    public String toString(){
        return "From " + originAirportId + " to " + destAirportId
                + ". Max delay: " + arrDelayNew
                + " Late and cancelled: " + cancelled;
    }


}
