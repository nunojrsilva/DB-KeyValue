import io.atomix.utils.net.Address;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.serializer.Serializer;

import java.lang.reflect.Array;
import java.time.Duration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;


public class ClienteStub {

   private HashMap<Address, Participante> coordenadores = new HashMap<>();
   private ArrayList<Address> coordEnderecos = new ArrayList<>();
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
        Participante part = new Participante();
        Address partEnd = Address.from("localhost:23451");
        part.endereco = partEnd;
        coordenadores.put(partEnd,part);
        coordEnderecos.add(partEnd);
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

    private CompletableFuture<Void> enviaMensagem(byte[] m, String assunto, Address a){
        Participante part = coordenadores.get(a);

        CompletableFuture<Void> meu = new CompletableFuture<Void>();

        ArrayList<CompletableFuture<Void>> esperarFuturo = new ArrayList<>(part.espera);
        CompletableFuture<Void>[] esperar = esperarFuturo.toArray(new CompletableFuture[esperarFuturo.size()]);
        part.espera.add(meu);

        return CompletableFuture.allOf(esperar).thenAccept(v -> {
            try{
                ms.sendAsync(a,assunto,m).thenAccept(msR -> {
                    meu.complete(null);
                });
            }
            catch(Exception e){
                System.out.println("Erro enviar mensagem: " + e); //podemos é por a remover
                meu.complete(null); //quando estiver completo
            }
        });
    }


    public void verificaPedido(int i, Address ad){
        System.out.println("Verificar pedido!");

        Pedido p = mapaPedidos.get(i);
        if(p instanceof PedidoPut){
            PedidoPut pp = (PedidoPut)p;

            if(pp.finalizado){
                resultadoPedidos.get(i).complete(pp.resultado);
            }
            else{
                enviaMensagem(s.encode(pp), "put", ad);

                //reenviar mensagem
                es.schedule(() -> {
                    verificaPedido(pp.id, ad);
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
        PedidoPut pp = new PedidoPut(values, pedidoAtual++);
        mapaPedidos.put(pp.id, pp);
        resultadoPedidos.put(pp.id, res);

        enviaMensagem(s.encode(pp), "put", coordenadores.get(coordEnderecos.get(coordAtual)).endereco);
        //ms.sendAsync(coordenadores.get(coordEnderecos.get(coordAtual)).endereco,"put", s.encode(pp));

        es.schedule(() -> {
            verificaPedido(pp.id, coordenadores.get(coordEnderecos.get(coordAtual)).endereco);
        },8, TimeUnit.SECONDS);

        coordAtual = (coordAtual + 1) % coordEnderecos.size();
        return res;
    }

    public CompletableFuture<Map<Long,byte[]>> get(Collection<Long> keys){
       //depois implementamos estes!
        return CompletableFuture.completedFuture(null);
    }

}
