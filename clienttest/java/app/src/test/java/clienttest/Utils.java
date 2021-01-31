package clienttest;

public class Utils {
    static final String NODE_HOST = "CLIENT_TEST_NODE_HOST";
    static final String NODE_PORT = "CLIENT_TEST_NODE_PORT";

    public static String getNodeHost() {
        var host = System.getenv(NODE_HOST);
        if (host == null || host.isEmpty()) {
            return "localhost";
        }
        return host;
    }

    public static int getNodePort() {
        var port = System.getenv(NODE_PORT);
        if (port == null || port.isEmpty()) {
            return 5299;
        }
        return Integer.parseInt(port);
    }
}
