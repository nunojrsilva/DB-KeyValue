import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

public class TwoPLocking implements Locking {
    Collection<Long> chavesLocked = new TreeSet<Long>();
    private ArrayList<TransactionID> locksAtuais = new ArrayList<>();
    private TreeSet<LockGlobal> filaLock = new TreeSet<LockGlobal>((o1, o2) -> {

        LockGlobal lock1 = (LockGlobal) o1;
        LockGlobal lock2 = (LockGlobal) o2;

        if(lock1.lockID > lock2.lockID){
            return 1;
        }
        if(lock1.lockID < lock2.lockID){
            return -1;
        }
        return 0;

        //return lock1.coordenador.toString().compareTo(lock2.coordenador.toString());
    });

    @Override
    public CompletableFuture<Void> lock(LockGlobal l) {
        System.out.println("Novo pedido de lock: " + l.xid + "! " + l.items());
        if(Collections.disjoint(chavesLocked,(Collection<Long>)l.items())){
            chavesLocked.addAll((Collection<Long>)l.items());
            locksAtuais.add(l.xid);
            l.obtido.complete(null);
        }
        return l.obtido;
    }

    @Override
    public void unlock(LockGlobal l) {
        if(!locksAtuais.contains(l.xid)){
            filaLock.removeIf(lock -> (lock.xid.equals(l.xid)));
            return;
        }
        chavesLocked.removeAll((Collection<Long>)l.items());

        ArrayList<LockGlobal> auxiliar = new ArrayList<>();

        for(LockGlobal aux: filaLock){
            if(Collections.disjoint(chavesLocked,(Collection<Long>)aux.items())){
                chavesLocked.addAll((Collection<Long>)aux.items());
                aux.obtido.complete(null);
                auxiliar.add(aux);
                locksAtuais.add(aux.xid);
            }
        }

        filaLock.removeAll(auxiliar);
    }
}
