package cs3213.jms.order;

import java.io.IOException;

public class OrderFinetune extends Filter {
    
    public void process() {
        while (true) {
            Order order = in.read();
            if (order == null) break;
                
            System.out.println("[OF] Receiving order: " + order.toString());

            // fine tune specifications of the devices in the order...

            out.write(order);
            System.out.println("[OF] Sending order: " + order.toString());
        }
    }
    
    public static void main(String[] args) {
        try {
            OrderFinetune os = new OrderFinetune();
            os.setPipeIn(new KeyboardPipe());
            
            if (args.length > 0 && args[0].equals("tcp"))
                os.setPipeOut(new TcpPipe(41020, "127.0.0.1", 20140));
            else
                os.setPipeOut(new JmsPipe("SimpleConnectionFactory", "SimpleQueue"));

            os.process();
        } catch (Exception e) {
            System.err.println("An exception has occurred: " + e.getMessage());
        }
    }
}
