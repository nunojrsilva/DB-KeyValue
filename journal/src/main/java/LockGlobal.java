import java.util.concurrent.CompletableFuture;

public abstract class LockGlobal{
    public CompletableFuture<Void> completo;
    public TransactionID xid;
    public int lockid;

    public LockGlobal(TransactionID xid, int lockid) {
        this.xid = xid;
        this.lockid = lockid;
    }

    abstract Object items();
}
