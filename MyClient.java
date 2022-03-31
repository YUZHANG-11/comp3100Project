import java.io.*;
import java.net.*;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.List;

public class MyClient {
    private Socket socket;
    private DataOutputStream out;
    private BufferedReader in;
    private CharBuffer cb;

    public MyClient() {
        try {
            socket = new Socket("127.0.0.1", 50000);
            cb = CharBuffer.allocate(40960);
            out = new DataOutputStream(socket.getOutputStream());
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String sendMessage(String cmd) {
        byte[] cmdBytes = (cmd + "\n").getBytes();
        String response = "";
        try {
            out.write(cmdBytes);
            in.read(cb);
            cb.flip();
            response = cb.toString();
            System.out.println(cmd + " response: " + response);
            cb.clear();
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public void close() {
        try {
            out.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<ServerNode> getServerList(String servers) {
        String[] lines = servers.split("\n");
        List<ServerNode> serverNodeList = new ArrayList<>();
        for (String line : lines) {
            String[] elements = line.split(" ");
            ServerNode serverNode = new ServerNode();

            serverNode.setServerType(elements[0]);
            serverNode.setServerID(Integer.parseInt(elements[1]));
            serverNode.setState(elements[2]);
            serverNode.setCurStartTime(Integer.parseInt(elements[3]));
            serverNode.setCore(Integer.parseInt(elements[4]));
            serverNode.setMemory(Integer.parseInt(elements[5]));
            serverNode.setDisk(Integer.parseInt(elements[6]));
            serverNode.setwJobs(Integer.parseInt(elements[7]));
            serverNode.setrJobs(Integer.parseInt(elements[8]));

            serverNodeList.add(serverNode);
        }
        return serverNodeList;
    }

    public static void main(String[] args) {

        MyClient client = new MyClient();

        client.sendMessage("HELO");
        client.sendMessage("AUTH " + System.getProperty("user.name"));
        String jobStr = client.sendMessage("REDY");
        client.sendMessage("GETS All");
        String servers = client.sendMessage("OK");
        List<ServerNode> nodeList = client.getServerList(servers);
        List<ServerNode> biggestCoreNodeList = new ArrayList<>();
        for (ServerNode node : nodeList) {
            if (biggestCoreNodeList.isEmpty() || (biggestCoreNodeList.get(0).getCore() == node.getCore()
                    && biggestCoreNodeList.get(0).getServerType().equals(node.getServerType()))) {
                biggestCoreNodeList.add(node);
            } else if (biggestCoreNodeList.get(0).getCore() < node.getCore()) {
                biggestCoreNodeList.clear();
                biggestCoreNodeList.add(node);
            }
        }
        client.sendMessage("OK");
        int round = 0;
        while (!jobStr.equals("NONE\n")) {
            if (jobStr.startsWith("JOBN")) {
                Job job = client.getJobFromString(jobStr);
                client.sendMessage("SCHD " + job.getJobID() + " " + biggestCoreNodeList.get(round).getServerType() + " "
                        + biggestCoreNodeList.get(round).getServerID());
                if (round < biggestCoreNodeList.size() - 1) {
                    round++;
                } else {
                    round = 0;
                }
            }
            jobStr = client.sendMessage("REDY");
        }
        client.sendMessage("QUIT");
        client.close();
    }

    private Job getJobFromString(String jobStr) {
        Job job = new Job();
        String[] elements = jobStr.split(" ");
        job.setSubmitTime(Integer.parseInt(elements[1]));
        job.setJobID(Integer.parseInt(elements[2]));
        job.setEstRuntime(Integer.parseInt(elements[3]));
        job.setCore(Integer.parseInt(elements[4]));
        job.setMemory(Integer.parseInt(elements[5]));
        job.setDisk(Integer.parseInt(elements[6].replace("\n", "")));
        return job;
    }


    

}