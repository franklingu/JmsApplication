package cs3213.jms.order;

public abstract class Filter {
    protected IPipe in, out;

    public void setPipeIn(IPipe in) {
        this.in = in;
    }
    
    public void setPipeOut(IPipe out) {
        this.out = out;
    }
    
    public abstract void process();
}
