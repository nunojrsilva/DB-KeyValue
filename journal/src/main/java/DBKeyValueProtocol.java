import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
class Msg {
    TransactionID id;
    Object valores;

    public Msg(TransactionID id, Object valores) {
        this.id = id;this.valores = valores;
    }
}

//mensagem para ser enviado do coordenador ao participante para o commit
class MsgCommit extends Msg {
    Object valores;

    public MsgCommit(TransactionID id, Object valores) {
        super(id,valores);
        this.valores = valores;
    }
}

//mensagem para ser enviada do coordenador para o participante e vice-versa durante um pedido get

class MsgGet {

    String idPedidoGet;
    Object valores; // Coordenador -> Participante : Vai a collection de chaves , ao contrario vem a resposta

    public MsgGet (String idPedidoGet, Object valores) {

        this.idPedidoGet = idPedidoGet;
        this.valores = valores;
    }
}


class LogEntry {
    public TransactionID xid;
    public String data;
    public Object valores;
    public PedidoID pedido;

    public LogEntry() {}
    public LogEntry(TransactionID xid, String data, Object valores) {
        this.xid=xid;
        this.data=data;
        this.valores = valores;
    }

    public LogEntry(TransactionID xid, String data, Object valores, PedidoID p) {
        this.xid=xid;
        this.data=data;
        this.valores = valores;
        pedido = p;
    }

    @Override
    public String toString() {
        if(valores == null){
            System.out.println("Valores é nulo!");
        }
        String mapa = null;
        if(valores != null){
            HashMap<Long,byte[]> val = (HashMap<Long,byte[]>)valores;
            mapa = "{";

            for(Map.Entry<Long,byte[]> aux: val.entrySet()){

                mapa += aux.getKey() + "=" + new String(aux.getValue(), StandardCharsets.UTF_8) + ",";
            }
            mapa += "}";
        }
        return "LogEntry{" +
                "xid=" + xid +
                ", data='" + data + '\'' +
                ", valores=" + mapa  +
                ", pedido=" + pedido +
                '}';
    }
}

class GetGestao {
    HashMap<Address, Object> valoresDevolver;
    Collection <Address> participantes;
    PedidoGet pg;
    Address cliente;

    public GetGestao(Collection<Address> participantes, PedidoGet pg, Address cliente) {
        this.valoresDevolver = new HashMap<>();
        this.participantes = participantes;
        this.pg = pg;
        this.cliente = cliente;
    }

    public void adicionaResposta (Address a, Object res) {

        this.valoresDevolver.put(a, res);
    }

    public boolean finalizado () {

        Collection <Address> jaResponderam = this.valoresDevolver.keySet();

        for (Address a : participantes) {
            if (!jaResponderam.contains(a))
                return false;

        }
        return true;
    }

}

interface Pedido{
    boolean finalizado();
    void finaliza(Object o);
}

class PedidoGet implements Pedido{
    public Object keys;
    public boolean finalizado;
    public Object resultado;
    public String id;

    public PedidoGet ( Object keys, String id) {
        this.keys = keys;
        this.id = id;

    }

    public boolean finalizado(){

        return finalizado;
    }

    public void finaliza(Object res){
        finalizado = true;
        resultado = res;
    }

}

class PedidoPut implements Pedido{
    public boolean finalizado = false;
    public Object valores;
    public boolean resultado;
    public String id;

    public PedidoPut(Object valores, String id) {
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
                        TransactionID.class,
                        PedidoID.class,
                        Msg.class,
                        MsgCommit.class,
                        LogEntry.class,
                        Pedido.class,
                        PedidoGet.class,
                        PedidoPut.class,
                        MsgGet.class
                )
                .build();
    }


}
