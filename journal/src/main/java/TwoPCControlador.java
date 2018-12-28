
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

class PedidoID{
    public String ad;
    public String id;

    public PedidoID(Address a, String i){
        ad = a.toString();
        id = i;
    }

    public boolean equals(Object o){
        if(o == null || !(o instanceof PedidoID)){
            return false;
        }

        PedidoID pi = (PedidoID)o;
        return ((id.equals(pi.id)) && (ad.equals(pi.ad)));
    }

    public String toString(){
        return "{id: " + id +
                ", address: " + ad +
                "}";
    }

}
public class TwoPCControlador extends TwoPC{

    private final int DELTA = 30;
    private ArrayList<Integer> paraCancelar = new ArrayList<>(); //ids com as proximas a serem canceladas
    private ArrayList<Boolean> possoCancelar = new ArrayList<>(); //valores bools que indicam se podemos ou não apagar
    private ArrayList<Integer> paraTerminar = new ArrayList<>(); //ids com as proximas a serem terminadas
    private ArrayList<Boolean> possoTerminar = new ArrayList<>(); //valores bools que indicam se podemos ou não terminar

    //private HashMap<Integer, ArrayList<LogEntry>> transacoesLog = new HashMap<>(); //para guardar as transacoes
    // dos logs

    private HashMap<Integer, Transaction> transacoes = new HashMap<>();
    private HashMap<PedidoID,Integer> pedidos = new HashMap<>();
    private int xid;

    //private Consumer<Msg> handlerMensagem;

    private void adicionaLock(Address a, Transaction t){

        for(Participante p: t.locksObtidos){
            if(p.endereco.toString().equals(a.toString())){
                System.out.println("Completo!");
                p.espera.complete(true);
                return;
            }
        }
    }
    private CompletableFuture<Void> enviaMensagem(Msg m, String assunto, Address a){
        System.out.println("Enviar " + assunto + " a: " + a);

        System.out.println("Vou enviar!");

        //return CompletableFuture.allOf(esperar).thenAccept(v -> {
        try{
            System.out.println("Vou mm tentar enviar");
            return ms.sendAsync(a,assunto,s.encode(m));
        }
        catch(Exception e){
            System.out.println("Erro enviar mensagem: " + e); //podemos é por a remover
        }
        return CompletableFuture.completedFuture(null);
        //});
    }


    private CompletableFuture<Void> enviaCommit(MsgCommit mc, List<Address> part, Transaction t){

        for(Address ad: part){
            try {
                mc.valores = t.participantes.get(ad);
                enviaMensagem(mc,"commit",ad);
            }catch(Exception e) {
                System.out.println(e);
            }
        }
        return CompletableFuture.completedFuture(null); //pode ser um allOf futuramente
    }

    private CompletableFuture<Void> enviaCommit(MsgCommit mc,Address ad) {

        return enviaMensagem(mc,"commit", ad);
    }

    private CompletableFuture<Void> enviaPrepared(Msg m, TreeSet<Participante> part){
        Participante abaixo = null;
        for(Participante p: part){
            try {
                if(abaixo == null){
                    System.out.println("Enviar prepared com size 0");
                    enviaMensagem(m,"prepared",p.endereco);
                }
                else{
                    System.out.println("Enviar prepared a: " + p.endereco);
                    System.out.println("Abaixo: " + abaixo.endereco);
                    abaixo.espera.thenAccept(v -> {
                        Transaction t = transacoes.get(m.id);

                        if (t != null && t.resultado.equals("I")) {
                            enviaMensagem(m, "prepared", p.endereco);
                        } else {
                            System.out.println("Outro resultado: ");
                        }



                    });
                }

            }catch(Exception e) {
                System.out.println(e);
            }
            abaixo = p;
        }
        return CompletableFuture.completedFuture(null); //pode ser um allOf futuramente
    }

    private CompletableFuture<Void> enviaAbort(Msg m, List<Address> part){

        for(Address ad: part){
            try {
                enviaMensagem(m,"abort",ad);
            }catch(Exception e) {
                System.out.println(e);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> enviaAbort(Msg m,Address ad) {

        return enviaMensagem(m,"abort",ad);
    }

    private void passouTempoTransacao(){
        int idT = paraCancelar.remove(0);
        boolean cancelo = possoCancelar.remove(0);

        if(cancelo) {
            writerLog.append(new LogEntry(idT,"A",null));
            transacoes.get(idT).resultado = "A";
            paraTerminar.add(idT);
            possoTerminar.add(false);
            System.out.println("Res: " + transacoes.get(idT).resultado);
            Msg paraMandarAux = new Msg(idT);
            Transaction tAux = transacoes.get(idT);
            for(Participante p: tAux.locksObtidos){
                p.espera.complete(null);
            } //depois podemos por um allOf antes do abort
            enviaAbort(paraMandarAux,new ArrayList<>(transacoes.get(idT).participantes.keySet()));


            es.schedule(() -> {
                cicloTerminar();
            }, DELTA, TimeUnit.SECONDS);
        }

    }

    private void analisaTransacaoControlador(){

        for(Transaction t: transacoes.values()){
            if(t.resultado.equals("I")){
                //mandar mensagem para todos prepared
                Msg paraMandar = new Msg(t.xid);
                paraCancelar.add(t.xid);
                possoCancelar.add(true);
                enviaPrepared(paraMandar,t.locksObtidos);


                //dar tempo para a resposta
                es.schedule( ()-> {
                    passouTempoTransacao();

                }, DELTA, TimeUnit.SECONDS);
            }
            else{
                if(t.resultado.equals("C")){
                    //mandar mensagem para todos commit
                    paraTerminar.add(t.xid);
                    possoTerminar.add(false);
                    System.out.println("Transacao efetuada com sucesso!");

                    //mandar mensagem a todos a dizer commit
                    MsgCommit msgC = new MsgCommit(t.xid,null);
                    enviaCommit(msgC,new ArrayList<>(t.participantes.keySet()),t);


                    es.schedule( ()-> {
                        cicloTerminar();
                    }, DELTA, TimeUnit.SECONDS);

                }
                else{
                    if(t.resultado.equals("A")){
                        //mandar mesnsagem para todos abort
                        Msg paraMandar = new Msg(t.xid);
                        paraTerminar.add(t.xid);
                        possoTerminar.add(false);

                        enviaAbort(paraMandar,new ArrayList<>(t.participantes.keySet()));


                        es.schedule( ()-> {
                            cicloTerminar();
                        }, DELTA, TimeUnit.SECONDS);

                    }
                    else{
                        //é F, dar transacao como terminada

                    }
                }
            }
        }
        //caso n seja nenhum é F, e n é preciso fazer nada (para já antes de apagar logs)
    }

    public void recuperaLogControlador(){
        xid = -1;

        while(readerLog.hasNext()) {
            LogEntry e = (LogEntry) readerLog.next().entry();
            if(e.xid > xid){
                xid = e.xid;
            }
            //verifica se a transacao já existe no mapa
            Transaction t = transacoes.get(e.xid);
            if(t == null) {
                //caso não exista cria e adiciona ao mapa
                HashMap<Address, HashMap<Long, byte[]>> part = null;
                //System.out.println(e.toString());
                if (e.valores != null) {
                    //se existirem valores (em caso de I ou C) entam vai buscar os participantes envolvidos e cada valor
                    // para o participante
                    part = participantesEnvolvidos(e.valores);
                }

                t = new Transaction(e.xid, e.data, part, e.pedido);
                transacoes.put(e.xid, t);
                pedidos.put(e.pedido,e.xid);
            }
            else{
                //caso exista apenas altera o resultado
                if(e.data.equals("F")){
                    boolean resPed = t.resultado.equals("C");
                    t.terminada.complete(resPed);
                }
                t.resultado = e.data;
            }
        }

        analisaTransacaoControlador();

    }


    public TwoPCControlador(Address[] e, int id, ManagedMessagingService ms){

        super(e,id,ms);

        System.out.println("TamEnd: " + end.length);

        recuperaLogControlador();

        System.out.println("Passei recupera log!");

        System.out.println("Sou o controlador!");
        //controlador tem de registar handler para ao receber um Prepared ou Abort

        ms.registerHandler("ok", (o,m) -> {
            System.out.println("Recebi um ok! " + o);
            try {
                Msg nova = s.decode(m);

                //pensar depois para o caso em que a transacao está terminada
                if(transacoes.get(nova.id).resultado.equals("F")){
                    return;
                }

                if(transacoes.get(nova.id).resultado.equals("I")){
                    System.out.println("Conflito no OK");
                }


                else{
                    //ainda não existe resultado
                    Transaction t = transacoes.get(nova.id);
                    t.quaisResponderam.add(o);


                    if(t.quaisResponderam.size() == t.participantes.size()){
                        //já responderam todos e pode-se mandar fazer commit
                        writerLog.append(new LogEntry(nova.id, "F", null));
                        boolean resPedido = t.resultado.equals("C");
                        t.resultado = "F";
                        int indiceAux = paraTerminar.indexOf(Integer.valueOf(nova.id));
                        possoTerminar.set(indiceAux, true);
                        t.terminada.complete(resPedido);
                        System.out.println("Transacao finalizada com sucesso!");
                    }
                }
            }


            catch(Exception exc){
                System.out.println("exc: " + exc);
            }
        }, es);

        ms.registerHandler("prepared", (o,m)->{
            System.out.println("Recebi prepared!" + o);
            try {
                Msg nova = s.decode(m);
                Transaction taux = transacoes.get(nova.id);

                //temos depois de escrever no log o lock
                adicionaLock(o,taux);

                //pensar depois para o caso em que a transacao está terminada
                System.out.println("Passei decode!");
                if(!taux.resultado.equals("I")){
                    //já existe resultado diferente de I e então pode-se mandar mensagem consoante esse resultado

                    if(!taux.resultado.equals("F")) {
                        //e o resultado n é F
                        if (taux.resultado.equals("A")) {
                            Msg paraMandar = new Msg(nova.id);
                            enviaAbort(paraMandar, o);
                        }

                        else{
                            MsgCommit paraMandar = new MsgCommit(nova.id,taux.participantes.get(o));
                            enviaCommit(paraMandar,o);
                        }
                    }
                }

                else{
                    System.out.println("No else!");
                    //ainda não existe resultado
                    Transaction t = transacoes.get(nova.id);
                    t.quaisResponderam.add(o);
                    System.out.println("Adicionei responderam");

                    if(t.quaisResponderam.size() == t.participantes.size()){
                        System.out.println("Resultado depoisA: " + transacoes.get(nova.id).resultado);
                        System.out.println("Já responderam todos!");
                        //já responderam todos e pode-se mandar fazer commit
                        writerLog.append(new LogEntry(nova.id, "C", t.participantes.values()
                                .stream().reduce(new HashMap<>(), (r,n) -> { r.putAll(n); return r;})));
                        t.resultado = "C";
                        t.quaisResponderam = new HashSet<>();
                        int indiceAux = paraCancelar.indexOf(Integer.valueOf(nova.id));
                        possoCancelar.set(indiceAux,false);
                        paraTerminar.add(nova.id);
                        possoTerminar.add(false);
                        System.out.println("Transacao efetuada com sucesso!");

                        //mandar mensagem a todos a dizer commit
                        MsgCommit msgC = new MsgCommit(nova.id, null);
                        enviaCommit(msgC,new ArrayList<>(t.participantes.keySet()),t);

                        System.out.println("Resultado depois: " + transacoes.get(nova.id).resultado);

                        es.schedule( ()-> {
                            cicloTerminar();
                        }, DELTA, TimeUnit.SECONDS);

                    }
                }
            }


            catch(Exception exc){
                System.out.println("exc: " + exc);
            }
        },es);

        ms.registerHandler("abort", (a,m)-> {
            System.out.println("Recebi abort:" + a);
            Msg nova = s.decode(m);
            System.out.println("Recebi abort: " + nova.id + "!" + a);

            if(transacoes.get(nova.id).resultado.equals("A")){
                //já tem resultado, pelo que já mandou para todos e só mandamos para o que enviou
                Msg paraMandar = new Msg(nova.id);
                enviaAbort(paraMandar,a);

            }
            else {
                if (transacoes.get(nova.id).resultado.equals("C") || transacoes.get(nova.id).resultado.equals("F")) {
                    //conflito nos resultados
                    System.out.println("Conflito nos resultados no controlador ao receber abort!");
                }
                else {
                    //pode-se por o resultado a A, pois um abortou
                    writerLog.append(new LogEntry(nova.id, "A", null));


                    transacoes.get(nova.id).resultado = "A";
                    int indiceAux = paraCancelar.indexOf(Integer.valueOf(nova.id));
                    possoCancelar.set(indiceAux, false);
                    paraTerminar.add(nova.id);
                    possoTerminar.add(false);
                    System.out.println("Transacao abortada com sucesso!");


                    //mandar mensagem para todos os participantes daquela transacao
                    Msg paraMandar = new Msg(nova.id);

                    enviaAbort(paraMandar, new ArrayList<>(transacoes.get(nova.id).participantes.keySet()));


                    es.schedule(() -> {
                        cicloTerminar();
                    }, DELTA, TimeUnit.SECONDS);


                }
            }
        }, es);

        ms.registerHandler("put",(a,m) -> {

            System.out.println("Recebi put");

            PedidoPut pp = s.decode(m);

            PedidoID pi = new PedidoID(a,pp.id);

            for(Map.Entry<PedidoID,Integer> mePi : pedidos.entrySet()){
                if(mePi.getKey().equals(pi)){
                    System.out.println("Pedido já existe! Decidir o que temos de fazer");
                    transacoes.get(mePi.getValue()).terminada
                        .thenAccept(res -> {
                            System.out.println("Terminada, posso mandar resultado!");
                            pp.resultado = res;
                            pp.finalizado = true;
                            System.out.println("Enviar a: " + a);
                            byte[] auxPut = s.encode(pp);
                            ms.sendAsync(a,"put",auxPut);
                            System.out.println("Enviei resposta!");
                    });
                    return;
                }
            }

            System.out.println("Nao existe!");
            try {
                HashMap<Long, byte[]> valores = new HashMap<>(pp.valores);

                writerLog.append(new LogEntry(++xid, "I", valores, pi));
                pedidos.put(pi, xid);

                System.out.println("Vou mandar!");

            //mandar mensagem para todos para iniciar transacao
            Msg paraMandar = new Msg(xid);
            HashMap<Address, HashMap<Long, byte[]>> participantes = participantesEnvolvidos(valores);
            Transaction novaTransacao = new Transaction(xid, "I", participantes,pi);
            novaTransacao.terminada = new CompletableFuture<Boolean>();
            transacoes.put(xid, novaTransacao);

            enviaPrepared(paraMandar, novaTransacao.locksObtidos); //depois aqui vai ter thenQQCoisa


            paraCancelar.add(paraMandar.id); //adicionar este id ao array para cancelar a transacao com o id
            possoCancelar.add(true); //para já podemos cancelar (até que alguem ponha resposta)

            es.schedule(() -> {
                try{
                    System.out.println("Passou tempo");
                    System.out.println("Esta terminado o tempo de: " + paraMandar.id);
                }
                catch(Exception exc){
                    System.out.println(exc);
                }
                passouTempoTransacao();
            }, DELTA, TimeUnit.SECONDS);

            System.out.println("Vou terminar");
            novaTransacao.terminada
                .thenAccept(res -> {
                    System.out.println("Terminada, posso mandar resultado!");
                    pp.resultado = res;
                    pp.finalizado = true;
                    System.out.println("Enviar a: " + a);
                    byte[] auxPut = s.encode(pp);

                    ms.sendAsync(a,"put",auxPut);
                    System.out.println("Enviei resposta!");
            });
        }catch(Exception exc){
                System.out.println("Exc: " + exc);
        }

        },es);
    }

    private void cicloTerminar(){
        int idT = paraTerminar.remove(0);
        boolean acabou = possoTerminar.remove(0);

        if(acabou) {
            System.out.println("Esquecer a Transacao!");
        }else{
            paraTerminar.add(idT);
            possoTerminar.add(false);


            Transaction t = transacoes.get(idT);
            ArrayList<Address> naoResponderam = new ArrayList<>(t.participantes.keySet());
            naoResponderam.removeAll(t.quaisResponderam);

            if(t.resultado.equals("A")){
                Msg msg = new Msg(t.xid);
                enviaAbort(msg,naoResponderam);

            }else{
                MsgCommit msg = new MsgCommit(t.xid, null);
                enviaCommit(msg,naoResponderam,t);

            }

            es.schedule( ()-> {
                cicloTerminar();
            }, DELTA, TimeUnit.SECONDS);
        }

    }

    public HashMap<Address,HashMap<Long,byte[]>> participantesEnvolvidos(HashMap<Long, byte[]> valores){
        HashMap<Address,HashMap<Long,byte[]>> participantes = new HashMap<>();
        for(Long aux : valores.keySet()){
            int resto = (int)(aux % (end.length-1)) + 1; //para já o coordenador n participa
            HashMap<Long,byte[]> auxiliar = participantes.get(end[resto]);
            if(auxiliar == null){
                auxiliar = new HashMap<>();
            }
            auxiliar.put(aux,valores.get(aux));
            participantes.put(end[resto],auxiliar);
        }
        return participantes;
    }

    /*public void iniciaTransacao(HashMap<Long,byte[]> valores){
        //depois tem de perguntar sempre se quer realizar transação
        xid++;
        //mandar mensagem para todos para iniciar transacao
        Msg paraMandar = new Msg(xid);
        writerLog.append(new LogEntry(xid,"I",valores));
        HashMap<Address,HashMap<Long,byte[]>> participantes = participantesEnvolvidos(valores);
        Transaction novaTransacao = new Transaction(xid, "I", participantes);
        novaTransacao.terminada = new CompletableFuture<Boolean>();
        transacoes.put(xid,novaTransacao);

        try {
            enviaPrepared(paraMandar,new ArrayList<>(participantes.keySet())).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            System.out.println(e.getMessage());
        }

        paraCancelar.add(paraMandar.id); //adicionar este id ao array para cancelar a transacao com o id
        possoCancelar.add(true); //para já podemos cancelar (até que alguem ponha resposta)

        es.schedule( ()-> {
            passouTempoTransacao();
        }, DELTA, TimeUnit.SECONDS);

    }*/
}
