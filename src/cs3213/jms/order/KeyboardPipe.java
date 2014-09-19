package cs3213.jms.order;

import java.util.Scanner;

public class KeyboardPipe implements IPipe {
    public Order read() {
        Scanner keyboard = new Scanner(System.in);
        System.out.println("Waiting for input...");
        System.out.print("Customer: ");
        String customerName = keyboard.nextLine();
        System.out.print("Product: ");
        String productName = keyboard.nextLine();
        return new Order(customerName, productName);
    }
    
    public void write(Order order) {
        // frankly, we need to split the interface here
        // but for the sake of simplicity (and laziness), we don't do that for now.
        
        System.err.println("Cannot write to a keyboard pipe.");
    }
    
    public void close() {
        
    }
}
