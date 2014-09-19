package cs3213.jms.order;

import java.net.Socket;
import java.net.ServerSocket;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class TcpPipe implements IPipe {
    
    private ServerSocket input;
    Socket server;
    
    private Socket output;
    private String dstAddress;
    private int dstPort;
    
    TcpPipe(int srcPort, String dstAddress, int dstPort) throws IOException {
        this.input = new ServerSocket(srcPort);
        System.out.println("Waiting for connections at port " + srcPort + "...");
     
        this.dstAddress = dstAddress;
        this.dstPort = dstPort;
    }    
    
    @Override
    public void write(Order order) {
        try {
            if (output == null) 
                output = new Socket(dstAddress, dstPort);
            
            OutputStreamWriter out = new OutputStreamWriter(output.getOutputStream());
            out.write(order.toString() + "\n");
            out.flush();
            
        } catch (IOException e) {
            System.err.println("An exception has occurred: " + e.getMessage());
        }
    }
    
    @Override
    public Order read() {
        try {
            if (server == null) 
                server = input.accept();
            
            BufferedReader reader = new BufferedReader(new InputStreamReader(server.getInputStream()));
            String line = reader.readLine();            
            if (line == null) return null;
            return Order.fromString(line);
            
        } catch (IOException e) {
            System.err.println("An exception has occurred: " + e.getMessage());
        }
        return null;
    }
    
    @Override
    public void close() {
        try {
            input.close();
            if (output != null)
                output.close();
        } catch (IOException e) {
            System.err.println("An exception has occurred: " + e.getMessage());
        }
    }
}
