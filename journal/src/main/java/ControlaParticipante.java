import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

public class ControlaParticipante implements InterfaceParticipante{

    public HashMap<Long,byte[]> valores = new HashMap<>();

    public ControlaParticipante(){

    }

    public Object devolveValores(Object chavesInput, Object valoresInput) {
        Collection<Long> chaves = (Collection<Long>)chavesInput;
        HashMap<Long,byte[]> valores = (HashMap<Long, byte[]>)valoresInput;

        HashMap<Long,byte[]> res = new HashMap<>();
        chaves.forEach(a -> res.put(a,valores.get(a)));

        return res;
    }

    public Object atualizaValores(Object novos) {
        HashMap<Long,byte[]> valN = (HashMap<Long, byte[]>)novos;

        valores.putAll(valN);
        return valores;
    }

    public Object getValores() {
        return valores;
    }

}
