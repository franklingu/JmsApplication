package cs3213.jms.order;

import java.io.IOException;

public class ProductMaker extends Filter {
    
    public void process() {
        Order order;
        
        while (true) {
            order = in.read();
            if (order == null) {
                //System.err.println("[PM] Empty order. Waiting...");
                
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {}  
                
                continue;
            }
            
            System.out.println("[PM] Receiving order: " + order.toString());
            
            // making the products...
            
        }
    }
    
    public static void main(String[] args) {
        try {
            ProductMaker or = new ProductMaker();            
            
            if (args.length > 0 && args[0].equals("tcp"))
                or.setPipeIn(new TcpPipe(20140, "127.0.0.1", 41020));
            else
                or.setPipeIn(new JmsPipe("SimpleConnectionFactory", "SimpleQueue"));
            
            or.process();
        } catch (Exception e) {
            System.err.println("An exception has occurred: " + e.getMessage());
        }
    }
}
