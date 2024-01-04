package src.util;

public class SpeedAggregator {
    public Long count;
    public Double sum;
    public SpeedAggregator(){
        count = 0L;
        sum = 0D;
    }
    public SpeedAggregator(Long count, Double sum){
        this.count = count;
        this.sum = sum;
    }
}
