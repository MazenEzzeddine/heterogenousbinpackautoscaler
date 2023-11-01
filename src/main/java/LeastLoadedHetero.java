import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class LeastLoadedHetero {
    private List<Partition> partitions;
    private float factor;
    private List<Double> rates;



    public LeastLoadedHetero(List<Partition> partitions, float factor, List<Double> rates) {
        this.partitions = partitions;
        this.factor = factor;
        this.rates = rates;
    }


    public List<Consumer> performLeastLoaded() {
        partitions.sort(Comparator.reverseOrder());
        float lagUnpacked = 0.0f;
        float arrivalUnpacked = 0.0f;

        for(int i = 0; i < partitions.size(); i++) {
            lagUnpacked += partitions.get(i).getLag();
            arrivalUnpacked += partitions.get(i).getArrivalRate();
        }


        List<Consumer> consumers = new ArrayList<>();
        int consCount = 1;

        while(true){
            for (int i = 0; i < consCount-1; i++) {
                consumers.get(i).removeAssignment();
            }

            int  index = chooseConsumerUsingHeuritic(arrivalUnpacked,lagUnpacked, rates);
            Consumer cons =  new Consumer(rates.get(index));
            cons.setId(String.valueOf(consCount-1));
            consumers.add(cons);
            arrivalUnpacked = 0.0f;
            lagUnpacked = 0.0f;
            for (int i = 0; i < partitions.size(); i++) {
                lagUnpacked += partitions.get(i).getLag();
                arrivalUnpacked += partitions.get(i).getArrivalRate();
            }
            int i;
            for ( i = 0; i < partitions.size(); i++) {
                Collections.sort(consumers, Collections.reverseOrder());
                int j;
                for ( j = 0; j < consCount; j++) {

                    if (partitions.get(i).getArrivalRate() <= consumers.get(j).getRemainingCapacity() &&
                            partitions.get(i).getLag() <= consumers.get(j).getRemainingCapacity()) {
                        arrivalUnpacked -= partitions.get(i).getArrivalRate();
                        lagUnpacked -= partitions.get(i).getLag();
                        consumers.get(j).assign(partitions.get(i));
                        break;
                    }
                }
                if (j==consCount) {
                    consCount +=1;
                    break;
                }

            }
            if(i==partitions.size()) {
                break;
            }

        }

        return consumers;

    }












    private int chooseConsumerUsingHeuritic(float arrivalUnpacked, float lagUnpacked, List<Double> rates) {
        Collections.sort(rates);
        for (int i = 0; i < rates.size(); i++) {
            if (lagUnpacked <= rates.get(i) &&  arrivalUnpacked <= rates.get(i) )
                return i;
        }
        return rates.size()-1;
    }


    public List<Partition> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<Partition> partitions) {
        this.partitions = partitions;
    }

    public float getFactor() {
        return factor;
    }

    public void setFactor(float factor) {
        this.factor = factor;
    }

    public List<Double> getRates() {
        return rates;
    }

    public void setRates(List<Double> rates) {
        this.rates = rates;
    }
}
