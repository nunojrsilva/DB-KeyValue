import java.util.HashMap;
public interface InterfaceParticipante {

    public Object devolveValores(Object chavesInput);

    public Object atualizaValores(Object novos);

    public Object getValores();

    public LockGlobal novoLock(TransactionID xid, Object o);
}
