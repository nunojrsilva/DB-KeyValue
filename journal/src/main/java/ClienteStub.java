import io.atomix.utils.net.Address;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import java.time.Duration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

interface Pedido{
    Address getCoordenador();
    boolean finalizado();
    void finaliza(Object o);
}

class PedidoGet implements Pedido{
    public Collection<Long> keys;
    public Address coordenador;
    public boolean finalizado;
    public HashMap<Long,byte[]> resultado;

    public Address getCoordenador(){
        return coordenador;
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
    public Address coordenador;
    public boolean finalizado = false;
    public Map<Long,byte[]> valores;
    public boolean resultado;
    public CompletableFuture<Boolean> cf;

    public PedidoPut(Address coordenador, Map<Long, byte[]> valores, CompletableFuture<Boolean> cf) {
        this.coordenador = coordenador;
        this.valores = valores;
        this.cf = cf;
    }

    public Address getCoordenador(){
        return coordenador;
    }
    public boolean finalizado(){
        return finalizado;
    }
    public void finaliza(Object res){
        finalizado = true;
        resultado = (boolean)res;
    }

}

public class ClienteStub {

   private Address[] coordenadores = {Address.from("localhost:23451")};
   private int atual= 0;
   private ManagedMessagingService ms;
   private Serializer s;
   private ScheduledExecutorService es;
   private ArrayList<Pedido> pedidos = new ArrayList<Pedido>();

   public ClienteStub(ManagedMessagingService ms){
        this.ms = ms;
        //inicializar Serializer
   }

   public void verificaPedido(){
       Pedido p = pedidos.remove(0);

       if(p instanceof PedidoPut){
           PedidoPut pp = (PedidoPut) p;
           if(pp.finalizado){
                pp.cf.complete(pp.resultado);
           }
           else{
               //voltar a mandar
               pedidos.add(pp);

               es.schedule(() -> {
                   verificaPedido();
               },20, TimeUnit.SECONDS);
           }
       }
       else{
           if(p instanceof PedidoGet){

           }
       }
   }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values){
        //
        CompletableFuture<Boolean> res = new CompletableFuture<Boolean>();
        PedidoPut pp = new PedidoPut(coordenadores[atual],values,res);
        pedidos.add(pp);

        ms.sendAndReceive(coordenadores[atual],"put", s.encode(values), Duration.ofSeconds(20))
                .thenApply(aux -> {
                    boolean resposta = s.decode(aux);
                    return res.completedFuture(resposta);
                }, es);

        es.schedule(() -> {
            verificaPedido();
        },20, TimeUnit.SECONDS);

        return res;


    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){
       //depois implementamos estes!
    }

}
