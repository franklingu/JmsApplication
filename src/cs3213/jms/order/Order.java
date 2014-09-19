package cs3213.jms.order;

public class Order {
    private String customerName;
    private String productName;
    
    public Order(String customerName, String productName) {
        this.customerName = customerName;
        this.productName = productName;
    }
    
    public static Order fromString(String s) {
        String[] data = s.split(",");
        String customerName = data[0];
        String productName = data[1];
        return new Order(customerName, productName);
    }
    
    public String toString() {
        return customerName + "," + productName;
    }
}
