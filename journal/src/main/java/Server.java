import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Server {
    public TwoPCParticipante participante;
    public TwoPCControlador controlador;

    private InterfaceControlador interfaceControlador;
    private InterfaceParticipante interfaceParticipante;

    private ScheduledExecutorService esParticipante;
    private ScheduledExecutorService esControlador;
    private Serializer s = DBKeyValueProtocol.newSerializer();

    private Address meuEnd;
    private ArrayList<Address> servidores = new ArrayList<>();
    private ManagedMessagingService ms;

    private HashMap<String, GetGestao> pedidosGetExecucao = new HashMap<>();

    private CompletableFuture<Void> enviaMensagemGet(MsgGet msgGet, String assunto, Address a){



        System.out.println("Enviar mensagem com pedido get, assunto : " + assunto + " a: " + a);


        //return CompletableFuture.allOf(esperar).thenAccept(v -> {
        try{
            System.out.println("Vou mm tentar enviar");
            return ms.sendAsync(a, assunto, s.encode(msgGet));
        }
        catch(Exception e){
            System.out.println("Erro enviar mensagem: " + e); //podemos é por a remover
        }
        return CompletableFuture.completedFuture(null);
        //});
    }

    public void registaHandlersCoordenador(){
        ms.registerHandler("put", (a,m) -> {
            System.out.println("Recebi put!");
            PedidoPut pp = s.decode(m);
            PedidoID pi = new PedidoID(a, pp.id);
            System.out.println("Vou ver se existe!");
            try{
                System.out.println(controlador.pedidos);
            }
            catch(Exception ec){
                System.out.println(ec);
            }
            try {
                for (Map.Entry<PedidoID, TransactionID> mePi : controlador.pedidos.entrySet()) {
                    //System.out.println("Pedido: " + mePi.getValue());
                    if (mePi.getKey().equals(pi)) {
                        System.out.println("Pedido já existe! Decidir o que temos de fazer");
                        controlador.transacoes.get(mePi.getValue()).terminada
                                .thenAccept(res -> {
                                    System.out.println("Terminada, posso mandar resultado!");
                                    pp.resultado = res;
                                    pp.finalizado = true;
                                    System.out.println("Enviar a: " + a);
                                    byte[] auxPut = s.encode(pp);
                                    ms.sendAsync(a, "put", auxPut);
                                    System.out.println("Enviei resposta!");
                                });
                        return;
                    }


                }


                System.out.println("Nao existe!");

                System.out.println("Vou terminar");
                controlador.iniciaTransacao(a, pp.id, pp.valores)
                        .thenAccept(res -> {
                            System.out.println("Terminada, posso mandar resultado!");
                            pp.resultado = res;
                            pp.finalizado = true;
                            System.out.println("Enviar a: " + a);
                            byte[] auxPut = s.encode(pp);

                            ms.sendAsync(a, "put", auxPut);
                            System.out.println("Enviei resposta!");
                        });

            }catch(Exception exc) {
                System.out.println(exc);
            }

        },esControlador);

        ms.registerHandler("get", (a, m) -> {

            System.out.println("Sou o controlador e recebi pedido get vindo do stub");

            PedidoGet pg = s.decode(m);

            HashMap<Address, Object> divisao = interfaceControlador.participantesGet(pg.keys);

            // Classe para gestão interna dos pedidos get por parte do coordenador

            GetGestao g = new GetGestao(divisao.keySet(), pg, a);

            // Coloquei o pedido get no estado do controlador

            this.pedidosGetExecucao.put(pg.id, g);

            System.out.println("Já dividi as chaves, vou enviar para cada um dos participantes");

            System.out.println(divisao.size());

            for (Address ad : divisao.keySet()) {

                System.out.println("Enviar mensagem get a " + ad);

                Object collectionRespetiva = divisao.get(ad);

                MsgGet msg = new MsgGet(pg.id, collectionRespetiva);

                enviaMensagemGet(msg, "getCoordenador", ad);

            }


            esControlador.schedule(() -> {

                try {
                    System.out.println("Passou tempo, vou devolver uma exceção");

                    MsgGet msg = new MsgGet(pg.id, null);

                    ms.sendAsync(a, "getExcecao", s.encode(msg));

                }
                catch (Exception exc) {

                    System.out.println(exc);
                }

            }, 20, TimeUnit.SECONDS);

        }, esControlador);


        ms.registerHandler("getResposta", (a, m) -> {

            System.out.println("Sou o controlador e recebi resposta ao pedido get");

            MsgGet mg = s.decode(m);

            GetGestao g = this.pedidosGetExecucao.get(mg.idPedidoGet);

            System.out.println("Resposta : " + mg.valores);

            g.adicionaResposta(a, mg.valores);

            if (g.finalizado()) {

                Object o = interfaceControlador.juntaValores(g.valoresDevolver);

                g.pg.resultado = o;

                // Avisar o cliente que o pedido está concluído
                // Devo encapsular isto numa função?

                ms.sendAsync(g.cliente, "get", s.encode(g.pg));

            }



        }, esControlador);

    }

    public void registaHandlersParticipante(){
        ms.registerHandler("getCoordenador", (a,m) -> {


            System.out.println("Sou o participante e recebi um pedido get");

            MsgGet mg = s.decode(m);

            // Mapa para devolver ao coordenador

            Object val = new HashMap<>();


            Collection<Long> chaves = (Collection<Long>) mg.valores;

            // Pedir ao controlaParticipante os valores

            val = this.interfaceParticipante.devolveValores(chaves);

            System.out.println("Mapa val apos ser preenchido com valores : " + val.toString());


            // Crio uma nova msgGet com o id do pedidoGet, os valores que me foram pedidos

            MsgGet msgGet = new MsgGet(mg.idPedidoGet, val);

            // Envio essa msgGet para o coordenador, que foi quem me enviou a mensagem

            ms.sendAsync(a,"getResposta",s.encode(msgGet));


        }, esParticipante);
    }

    public Server(Address[] end, Address meuEnd){
        esControlador = Executors.newSingleThreadScheduledExecutor();
        esParticipante = Executors.newSingleThreadScheduledExecutor();
        this.meuEnd = meuEnd;
        for(Address a: end){
            servidores.add(a);
        }

        interfaceParticipante = new ControlaParticipante();
        interfaceControlador = new ControlaControlador(servidores);
        TwoPLocking locking = new TwoPLocking();
        ms = NettyMessagingService.builder()
                .withAddress(this.meuEnd)
                .build();
        ms.start();
        controlador = new TwoPCControlador(end, meuEnd, ms,
                DBKeyValueProtocol.newSerializer(),interfaceControlador, esControlador);
        participante = new TwoPCParticipante(end, meuEnd, ms,
                DBKeyValueProtocol.newSerializer(),interfaceParticipante,locking, esParticipante);

        registaHandlersCoordenador();
        registaHandlersParticipante();

    }
}
