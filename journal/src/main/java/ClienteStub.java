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
   private ArrayList<Integer> pedidos = new ArrayList<Integer>();
   private HashMap<Integer,Pedido> mapaPedidos = new HashMap<>();
   private HashMap<Integer,CompletableFuture<Boolean>> resultadoPedidos = new HashMap<>();

   public ClienteStub(ManagedMessagingService ms){
        this.ms = ms;
        //inicializar Serializer
   }

   public void verificaPedido(){
       System.out.println("Verificar pedido!");
       int idP = pedidos.remove(0);
       Pedido p = mapaPedidos.get(idP);

       if(p instanceof PedidoPut){
           System.out.println("Verificar pedido put!" + ((PedidoPut) p).id);
           PedidoPut pp = (PedidoPut) p;
           if(pp.finalizado){
                System.out.println("finalizado!");
                resultadoPedidos.get(pp.id).complete(pp.resultado);
           }
           else{
               //voltar a mandar
               pedidos.add(pp.id);
               /*ms.sendAndReceive(Address.from(pp.coordenador),"put", s.encode(pp), Duration.ofSeconds(20),es)
                   .thenAccept((aux) -> {
                       PedidoPut rpp = s.decode(aux);
                       pp.finalizado = true;
                       mapaPedidos.put(rpp.id,rpp);
               });
                */

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
        PedidoPut pp = new PedidoPut(coordenadores[coordAtual],values,pedidoAtual++);
        pedidos.add(pp.id);
        mapaPedidos.put(pp.id,pp);
        resultadoPedidos.put(pp.id,res);

        try {
            ms.sendAsync(Address.from(coordenadores[coordAtual]),"put", s.encode(pp)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        ms.registerHandler("put", (a,m)->{
            System.out.println("Está completo!");
            PedidoPut rpp = s.decode(m);
            System.out.println(pp.id);
            rpp.finalizado = true;
            mapaPedidos.put(rpp.id,rpp);
        },es);
          /*  .thenAccept(aux -> {
                System.out.println("Está completo!");
                PedidoPut rpp = s.decode(aux);
                rpp.finalizado = true;
                mapaPedidos.put(rpp.id,rpp);

        });
        /*ms.sendAndReceive(Address.from(coordenadores[coordAtual]),"put", s.encode(pp), Duration.ofSeconds(20),es)
                .whenComplete((dados,erro) -> {
                    if(erro!=null){
                        System.out.println("Deu erro!");
                        return;
                    }
                    PedidoPut rpp = s.decode(aux);
                    pp.finalizado = true;
                    mapaPedidos.put(rpp.id,rpp);
                    System.out.println(("Recebi resposta: " + s.decode(dados)));
                });
    */
        es.schedule(() -> {
            verificaPedido();
        },8, TimeUnit.SECONDS);

        return res;
    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){
       //depois implementamos estes!
        return CompletableFuture.completedFuture(null);
    }

}
