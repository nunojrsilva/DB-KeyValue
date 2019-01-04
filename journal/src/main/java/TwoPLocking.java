import java.util.*;
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

        if(locksAtuais.contains(l.xid)){
            //se contiver um lock nos atuais retorna um completablefuture completo
            return CompletableFuture.completedFuture(null);
        }

        for(LockGlobal laux: filaLock){
            if(laux.xid.equals(l.xid)){
                //entao ja tenho lock para aquela transacao
                //retorno o completablefuture desse lock
                return laux.obtido;
            }
        }

        if(Collections.disjoint(chavesLocked,(Collection<Long>)l.items())){
            chavesLocked.addAll((Collection<Long>)l.items());
            locksAtuais.add(l.xid);
            l.obtido.complete(null);
        }
        else{
            System.out.println("Vai para a fila!");
            filaLock.add(l);
        }
        return l.obtido;
    }

    @Override
    public void unlock(LockGlobal l) {
        System.out.println("Novo pedido de unlock!");
        if(!locksAtuais.contains(l.xid)){
            System.out.println("Lock nao se encontra nos atuais!");
            filaLock.removeIf(lock -> (lock.xid.equals(l.xid)));
            return;
        }
        System.out.println("Chaves antes unlock: " + chavesLocked);
        chavesLocked.removeAll((Collection<Long>)l.items());
        System.out.println("Chaves depois unlock: " + chavesLocked);

        ArrayList<LockGlobal> auxiliar = new ArrayList<>();

        System.out.println("Fila no unlock: " + filaLock);

        for(LockGlobal aux: filaLock){
            if(Collections.disjoint(chavesLocked,(Collection<Long>)aux.items())){
                chavesLocked.addAll((Collection<Long>)aux.items());
                System.out.println("Completar lock em fila: " + aux.xid);
                aux.obtido.complete(null);
                auxiliar.add(aux);
                locksAtuais.add(aux.xid);
            }
        }

        filaLock.removeAll(auxiliar);
    }
}
