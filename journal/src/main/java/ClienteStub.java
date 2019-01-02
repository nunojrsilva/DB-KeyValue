import io.atomix.utils.net.Address;
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.serializer.Serializer;

import java.lang.reflect.Array;
import java.time.Duration;

import java.util.*;
import java.util.concurrent.*;


public class ClienteStub {

   //private HashMap<Address, Participante> coordenadores = new HashMap<>();
   private ArrayList<Address> coordEnderecos = new ArrayList<>();
   private int coordAtual;
   private ManagedMessagingService ms;
   private Serializer s = DBKeyValueProtocol.newSerializer();
   private ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();
   private HashMap<String, Pedido> mapaPedidos = new HashMap<>();
   private HashMap<String, CompletableFuture<Boolean>> resultadoPedidos = new HashMap<>();
   private HashMap<String, CompletableFuture<Map<Long,byte[]>>> resultadoPedidosGet = new HashMap<>();

   public ClienteStub(ManagedMessagingService ms){
        this.ms = ms;
        //Participante part = new Participante();
        Address partEnd = Address.from("localhost:23451");
        Address partEnd2 = Address.from("localhost:23452");
        Address partEnd3 = Address.from("localhost:23453");

       //part.endereco = partEnd;
        //coordenadores.put(partEnd,part);
        coordEnderecos.add(partEnd);
        coordEnderecos.add(partEnd2);
        coordEnderecos.add(partEnd3);
        Random rand = new Random();

        int n = rand.nextInt(coordEnderecos.size());
        coordAtual = n;


        System.out.println("Novo clientstub! O coordenador atual é: " + coordEnderecos.get(coordAtual));
        //inicializar Serializer
        this.ms.registerHandler("put", (a,m)->{
           //System.out.println("Está completo o pedido put!");
           PedidoPut rpp = s.decode(m);
           //System.out.println(rpp.id);
           rpp.finalizado = true;
           mapaPedidos.put(rpp.id,rpp);
           resultadoPedidos.get(rpp.id).complete(rpp.resultado);
       },es);

       this.ms.registerHandler("get", (a,m)->{
           //System.out.println("Está completo o pedido Get!");
           PedidoGet rpg = s.decode(m);
           System.out.println(rpg.id);
           rpg.finalizado = true;
           mapaPedidos.put(rpg.id,rpg);
           resultadoPedidosGet.get(rpg.id).complete((HashMap<Long,byte[]>)rpg.resultado);
       },es);
   }

    private CompletableFuture<Void> enviaMensagem(byte[] m, String assunto, Address a){
        System.out.println("Enviar " + assunto + " a: " + a);

        System.out.println("Vou enviar!");

        //return CompletableFuture.allOf(esperar).thenAccept(v -> {
            try{
                System.out.println("Vou mm tentar enviar");
                return ms.sendAsync(a,assunto,m);
            }
            catch(Exception e){
                System.out.println("Erro enviar mensagem: " + e); //podemos é por a remover
            }
            return CompletableFuture.completedFuture(null);
        //});
    }


    public void verificaPedido(String i, Address ad){
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
        else {
            //o mesmo para o get
            /*PedidoGet pg = (PedidoGet) p;

            if (pg.finalizado) {
                resultadoPedidosGet.get(i).complete(pg.resultado);
            }
            else {
                enviaMensagem(s.encode(pg), "put", ad);

                //reenviar mensagem
                es.schedule(() -> {
                    verificaPedido(pg.id, ad);
                },8, TimeUnit.SECONDS);

            }*/
        }
   }

    public CompletableFuture<Boolean> put(Map<Long,byte[]> values) {
        //
        System.out.println("-------Novo pedido put-------------");
        CompletableFuture<Boolean> res = new CompletableFuture<Boolean>();
        String idPedido = UUID.randomUUID().toString();
        PedidoPut pp = new PedidoPut(values, idPedido);
        mapaPedidos.put(pp.id, pp);
        resultadoPedidos.put(pp.id, res);

        enviaMensagem(s.encode(pp), "put", coordEnderecos.get(coordAtual));
        //ms.sendAsync(coordenadores.get(coordEnderecos.get(coordAtual)).endereco,"put", s.encode(pp));
        int auxCoordenador = coordAtual;
        es.schedule(() -> {
            verificaPedido(pp.id, coordEnderecos.get(auxCoordenador));
        },8, TimeUnit.SECONDS);

        coordAtual = (coordAtual + 1) % coordEnderecos.size();
        return res;
    }

    public CompletableFuture<Map<Long,byte[]>> get (Collection<Long> keys){
       //depois implementamos estes!

        CompletableFuture<Map <Long, byte[]>> res = new CompletableFuture<>();
        String idPedido = UUID.randomUUID().toString();
        PedidoGet pg = new PedidoGet(keys, idPedido);

        mapaPedidos.put(pg.id, pg);

        resultadoPedidosGet.put(pg.id, res);

        enviaMensagem(s.encode(pg), "get", coordEnderecos.get(coordAtual));

        es.schedule(() -> {
            verificaPedido(pg.id, coordEnderecos.get(coordAtual));
        },8, TimeUnit.SECONDS);

        coordAtual = (coordAtual + 1) % coordEnderecos.size();
        return res;

    }

}
