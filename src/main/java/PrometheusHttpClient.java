import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/////////////////////////////////////////////////////
/////////////////////////////////////////////////////

public class PrometheusHttpClient implements Runnable {

    private static final Logger log = LogManager.getLogger(PrometheusHttpClient.class);

    static Long sleep;
    static String topic;
    static Long poll;
    static String BOOTSTRAP_SERVERS;
    public static String CONSUMER_GROUP;
    public static AdminClient admin = null;
    static Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap;
    static int size;
    static ArrayList<Partition> topicpartitions = new ArrayList<>();

    static double wsla = 5.0;

    static Map<Double, Integer> previousConsumers = new HashMap<>();
    static Map<Double, Integer> currentConsumers =  new HashMap<>();
    final static      List<Double> capacities = Arrays.asList(90.0, 230.0);
    public static List<Consumer> newassignment = new ArrayList<>();

    public static  Instant warmup = Instant.now();
    static Instant lastScaletime;
    ////////////////////////////////////////////////////////////////////////


    ////////////////////////////////////////////////////////////////////////





    private static void initialize() throws InterruptedException, ExecutionException {

        //Lag.readEnvAndCrateAdminClient();


        for (double c : capacities) {
            currentConsumers.put(c, 0);
            previousConsumers.put(c,0);
        }
        previousConsumers.put(90.0, 1);

        while (true) {
            log.info("Querying Prometheus");
            ArrivalProducer.callForArrivals();
            //Lag.getCommittedLatestOffsetsAndLag();
            log.info("--------------------");
            log.info("--------------------");

            youMightWanttoScaleTrial2();
            log.info("Sleeping for 1 seconds");
            log.info("******************************************");
            log.info("******************************************");
            Thread.sleep(1000);
        }
    }







    @Override
    public void run() {

        log.info("warmup");
        try {
            Thread.sleep(15*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        log.info("Warm up completed");

        try {
            initialize();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }




    public static void  youMightWanttoScaleTrial2(){
        log.info("Inside binPackAndScale ");
        List<Consumer> consumers = new ArrayList<>();

        Map<Double, List<Consumer>> currentConsumersByName = new HashMap<>();

        LeastLoadedHetero llh = new LeastLoadedHetero(ArrivalProducer.topicpartitions, 1.0f, capacities);
        List<Consumer> cons = llh.performLeastLoaded();


        for(double c : capacities) {
            currentConsumersByName.putIfAbsent(c, new ArrayList<>());
        }
        log.info("we currently need this consumer");
        log.info(cons);
        newassignment.clear();
        for (Consumer co: cons) {
            log.info(co.getCapacity());
            currentConsumers.put(co.getCapacity(), currentConsumers.get(co.getCapacity()) +1);
            currentConsumersByName.get(co.getCapacity()).add(co);
            // currentConsumersByName.put(co.getCapacity(), co);
        }
        for (double d : currentConsumers.keySet()) {
            log.info("current consumer capacity {}, {}", d, currentConsumers.get(d));
        }

        Map<Double, Integer> scaleByCapacity = new HashMap<>();
        Map<Double, Integer>  diffByCapacity = new HashMap<>();

        for (double d : currentConsumers.keySet()) {
            if (currentConsumers.get(d).equals(previousConsumers.get(d))) {
                log.info("No need to scale consumer of capacity {}", d);
            }
            int index=0;
            for (Consumer c:  currentConsumersByName.get(d)) {
                c.setId("cons"+(int)d+ "-" + index);
                index++;
                log.info(c.getId());
            }

            int factor = currentConsumers.get(d); /*- previousConsumers.get(d);*/
            int  diff = currentConsumers.get(d) - previousConsumers.get(d);
            log.info("diff {} for capacity {}", diff, d);
            diffByCapacity.put(d, diff);
            scaleByCapacity.put(d, factor);
            log.info(" the consumer of capacity {} shall be scaled to {}", d, factor);
        }

        newassignment.addAll(cons);

        // scale up
        for (double d : capacities) {
            if (scaleByCapacity.get(d) != null && diffByCapacity.get(d) > 0) {
                log.info("The statefulset {} shall be  scaled to {}", "cons" + (int) d, scaleByCapacity.get(d));

                log.info("cons" + (int) d);

                new Thread(() -> {
                    try (final KubernetesClient k8s = new KubernetesClientBuilder().build()) {
                        k8s.apps().statefulSets().inNamespace("default").withName("cons" + (int) d).scale(scaleByCapacity.get(d));
                    }
                }).start();
                lastScaletime = Instant.now();
            }
        }

        // scale down
        for (double d : capacities) {
            if (scaleByCapacity.get(d) != null && diffByCapacity.get(d) < 0) {
                log.info("The statefulset {} shall be  scaled to {}", "cons" + (int) d, scaleByCapacity.get(d));
                log.info("cons" + (int) d);

                new Thread(() -> {
                    try (final KubernetesClient k8s = new KubernetesClientBuilder().build()) {
                        k8s.apps().statefulSets().inNamespace("default").withName("cons" + (int) d).scale(scaleByCapacity.get(d));
                    }
                }).start();
                lastScaletime = Instant.now();

            }
        }

        for (double d : capacities) {
            previousConsumers.put(d, currentConsumers.get(d));
            currentConsumers.put(d, 0);
        }

    }



}
