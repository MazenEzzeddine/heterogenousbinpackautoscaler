import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {

        AssignmentServer server = new AssignmentServer(5002);
        PrometheusHttpClient controller = new PrometheusHttpClient();
        Thread serverthread = new Thread(server);
        Thread controllerthread = new Thread(controller);

        controllerthread.start();
        serverthread.start();
    }
}
