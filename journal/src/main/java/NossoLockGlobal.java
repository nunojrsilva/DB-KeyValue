import java.util.Collection;
import java.util.HashMap;

public class NossoLockGlobal extends LockGlobal {

    public Collection<Long> keys;

    public NossoLockGlobal(TransactionID xid, int lockid, Object o){
        super(xid,lockid);

        keys = ((HashMap<Long,byte[]>)o).keySet();
    }
    @Override
    Object items() {
        return keys;
    }

    //tem de implementar o equals igual ao do lock na class 2PCParticipante
}
