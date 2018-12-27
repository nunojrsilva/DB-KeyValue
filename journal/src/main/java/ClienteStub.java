import io.atomix.utils.net.Address;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;


public class ClienteStub {

   private String[] coordenadores = {"localhost:23451"};
   private int coordAtual= 0;
   private int pedidoAtual = 0;
   private ManagedMessagingService ms;
   private Serializer s = DBKeyValueProtocol.newSerializer();
   private ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();
   private HashMap<Integer,Pedido> mapaPedidos = new HashMap<>();
   private HashMap<Integer,CompletableFuture<Boolean>> resultadoPedidos = new HashMap<>();

   public ClienteStub(ManagedMessagingService ms, int pa){
        this.ms = ms;
        pedidoAtual = pa;
        //inicializar Serializer
        this.ms.registerHandler("put", (a,m)->{
           System.out.println("Está completo!");
           PedidoPut rpp = s.decode(m);
           System.out.println(rpp.id);
           rpp.finalizado = true;
           mapaPedidos.put(rpp.id,rpp);
           resultadoPedidos.get(rpp.id).complete(rpp.resultado);
       },es);
   }

   public void verificaPedido(int i){
        System.out.println("Verificar pedido!");

        Pedido p = mapaPedidos.get(i);
        if(p instanceof PedidoPut){
            PedidoPut pp = (PedidoPut)p;

            if(pp.finalizado){
                resultadoPedidos.get(i).complete(pp.resultado);
            }
            else{
                ms.sendAsync(Address.from(coordenadores[coordAtual]),"put", s.encode(pp));
                //reenviar mensagem
                es.schedule(() -> {
                    verificaPedido(pp.id);
                },8, TimeUnit.SECONDS);
            }
        }
        else{
            //o mesmo para o get
        }
   }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values) {
        //
        CompletableFuture<Boolean> res = new CompletableFuture<Boolean>();
        PedidoPut pp = new PedidoPut(coordenadores[coordAtual], values, pedidoAtual++);
        mapaPedidos.put(pp.id, pp);
        resultadoPedidos.put(pp.id, res);

        ms.sendAsync(Address.from(coordenadores[coordAtual]),"put", s.encode(pp));

        es.schedule(() -> {
            verificaPedido(pp.id);
        },8, TimeUnit.SECONDS);

        return res;
    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){
       //depois implementamos estes!
        return CompletableFuture.completedFuture(null);
    }

}
