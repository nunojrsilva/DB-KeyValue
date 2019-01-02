import java.util.concurrent.CompletableFuture;

public abstract class LockGlobal{
    public CompletableFuture<Void> obtido = new CompletableFuture<>();
    public TransactionID xid;
    public int lockID;

    public LockGlobal(TransactionID xid, int lockid) {
        this.xid = xid;
        this.lockID = lockid;
    }

    abstract Object items();
}
