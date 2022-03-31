import java.io.*;
import java.net.*;

public class MyClient {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("127.0.0.1", 50000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            out.write(("HELO\n").getBytes());
            out.flush();
            String response = in.readLine();
            System.out.println("HELO response= " + response);
            String username = System.getProperty("user.name");
            String command = "AUTH " + username + "\n";
            out.write(command.getBytes());
            out.flush();
            response = in.readLine();
            System.out.println("AUTH response= " + response);
            out.write(("REDY\n").getBytes());
            response = in.readLine();
            System.out.println("AUTH response= " + response);
            out.write(("QUIT\n").getBytes());
            out.flush();
            response = in.readLine();
            System.out.println("QUIT: " + response);
            out.close();
            socket.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}