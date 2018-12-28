import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
class Msg {
    int id;

    public Msg(int id) {
        this.id = id;
    }
}

//mensagem para ser enviado do coordenador ao participante para o commit
class MsgCommit extends Msg {
    HashMap<Long,byte[]> valores;

    public MsgCommit(int id, HashMap<Long, byte[]> valores) {
        super(id);
        this.valores = valores;
    }
}


class LogEntry {
    public int xid;
    public String data;
    public HashMap<Long,byte[]> valores;

    public LogEntry() {}
    public LogEntry(int xid, String data, HashMap<Long, byte[]> valores) {
        this.xid=xid;
        this.data=data;
        this.valores = valores;
    }

    @Override
    public String toString() {
        String mapa = null;
        if(valores != null){
            mapa = "{";

            for(Map.Entry<Long,byte[]> aux: valores.entrySet()){

                mapa += aux.getKey() + "=" + new String(aux.getValue(), StandardCharsets.UTF_8) + ",";
            }
            mapa += "}";
        }
        return "LogEntry{" +
                "xid=" + xid +
                ", data='" + data + '\'' +
                ", valores=" + mapa +
                '}';
    }
}

interface Pedido{
    boolean finalizado();
    void finaliza(Object o);
}

class PedidoGet implements Pedido{
    public Collection<Long> keys;
    public boolean finalizado;
    public HashMap<Long,byte[]> resultado;
    public int id;

    public PedidoGet ( Collection <Long> keys, int id) {
        this.keys = keys;
        this.id = id;

    }

    public boolean finalizado(){

        return finalizado;
    }

    public void finaliza(Object res){
        finalizado = true;
        resultado = (HashMap<Long,byte[]>)res;
    }

}

class PedidoPut implements Pedido{
    public boolean finalizado = false;
    public Map<Long,byte[]> valores;
    public boolean resultado;
    public int id;

    public PedidoPut(Map<Long, byte[]> valores, int id) {
        this.valores = valores;
        this.id = id;
    }

    public boolean finalizado(){
        return finalizado;
    }
    public void finaliza(Object res){
        finalizado = true;
        resultado = (boolean)res;
    }

}


public class DBKeyValueProtocol {

    public static Serializer newSerializer() {
        return Serializer.builder()
                .withTypes(
                        Msg.class,
                        MsgCommit.class,
                        LogEntry.class,
                        Pedido.class,
                        PedidoGet.class,
                        PedidoPut.class
                )
                .build();
    }


}
