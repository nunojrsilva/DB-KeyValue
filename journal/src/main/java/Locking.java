import java.util.concurrent.CompletableFuture;

public interface Locking {
    CompletableFuture<Void> lock(LockGlobal l);
    void unlock(LockGlobal l);
}
