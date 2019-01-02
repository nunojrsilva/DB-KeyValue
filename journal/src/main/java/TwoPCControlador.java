
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Function;
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

    private final int DELTA = 10;
    private final int TEMPOTRANSACAO = 20;
    /*private ArrayList<Integer> paraCancelar = new ArrayList<>(); //ids com as proximas a serem canceladas
    private ArrayList<Boolean> possoCancelar = new ArrayList<>(); //valores bools que indicam se podemos ou não apagar
    private ArrayList<Integer> paraTerminar = new ArrayList<>(); //ids com as proximas a serem terminadas
    private ArrayList<Boolean> possoTerminar = new ArrayList<>(); //valores bools que indicam se podemos ou não terminar
    */
    //private HashMap<Integer, ArrayList<LogEntry>> transacoesLog = new HashMap<>(); //para guardar as transacoes
    // dos logs

    private HashMap<TransactionID, Transaction> transacoes = new HashMap<>();
    private HashMap<PedidoID,TransactionID> pedidos = new HashMap<>();
    private int xid;
    /*private Function<Object,HashMap<Address,Object>> distribuiPorParticipante; //funcao que da os participantes
    private Function<HashMap<Address,Object>,Object> juntaValores; //funcao inversa à anterior
    //private Consumer<Msg> handlerMensagem;

    private Function<Object,HashMap<Address,Object>> participantesGet; //funcao que devolve
    // os participantes de um get
    */
    private InterfaceControlador controla;

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


    private CompletableFuture<Void> enviaCommit(MsgCommit mc, Transaction t){

        Participante abaixo = null;
        for(Participante p: t.locksObtidos){
            try {
                if(abaixo == null){
                    System.out.println("Enviar commit com size 0");
                    mc.valores = t.participantes.get(p.endereco);
                    enviaMensagem(mc,"commitCoordenador",p.endereco);
                }
                else{
                    System.out.println("Enviar commit a: " + p.endereco);
                    System.out.println("Abaixo no commit: " + abaixo.endereco);
                    abaixo.espera.thenAccept(v -> {
                        Transaction taux = transacoes.get(mc.id);

                        if (taux != null && taux.resultado.equals("C")) {
                            mc.valores = t.participantes.get(p.endereco);
                            enviaMensagem(mc,"commitCoordenador",p.endereco);
                        } else {
                            System.out.println("Outro resultado no commit: ");
                        }



                    });
                }

            }catch(Exception e) {
                System.out.println(e);
            }
            abaixo = p; //atualizar o que está antes dele
        }
        return CompletableFuture.completedFuture(null); //pode ser um allOf futuramente
    }

    private CompletableFuture<Void> enviaPrepared(Msg mc, Transaction t){

        Participante abaixo = null;
        for(Participante p: t.locksObtidos){
            try {
                if(abaixo == null){
                    System.out.println("Enviar prepared com size 0");
                    mc.valores = t.participantes.get(p.endereco);
                    enviaMensagem(mc,"preparedCoordenador",p.endereco);
                }
                else{
                    System.out.println("Enviar prepared a: " + p.endereco);
                    System.out.println("Abaixo no prepared: " + abaixo.endereco);
                    abaixo.espera.thenAccept(v -> {
                        Transaction taux = transacoes.get(mc.id);

                        if (taux != null && taux.resultado.equals("I")) {
                            mc.valores = t.participantes.get(p.endereco);
                            enviaMensagem(mc,"preparedCoordenador",p.endereco);
                        } else {
                            System.out.println("Outro resultado no prepared: ");
                        }



                    });
                }

            }catch(Exception e) {
                System.out.println(e);
            }
            abaixo = p; //atualizar o que está antes dele
        }
        return CompletableFuture.completedFuture(null); //pode ser um allOf futuramente
    }

    private CompletableFuture<Void> enviaCommit(MsgCommit mc,Address ad) {

        return enviaMensagem(mc,"commitCoordenador", ad);
    }

    /*private CompletableFuture<Void> enviaPrepared(Msg m, TreeSet<Participante> part){
        Participante abaixo = null;
        for(Participante p: part){
            try {
                if(abaixo == null){
                    System.out.println("Enviar prepared com size 0");
                    enviaMensagem(m,"preparedCoordenador",p.endereco);
                }
                else{
                    System.out.println("Enviar prepared a: " + p.endereco);
                    System.out.println("Abaixo: " + abaixo.endereco);
                    abaixo.espera.thenAccept(v -> {
                        Transaction t = transacoes.get(m.id);

                        if (t != null && t.resultado.equals("I")) {
                            enviaMensagem(m, "preparedCoordenador", p.endereco);
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
    }*/

    private CompletableFuture<Void> enviaAbort(Msg m, List<Address> part, Transaction t){

        for(Address ad: part){
            try {
                m.valores = t.participantes.get(ad);
                enviaMensagem(m,"abortCoordenador",ad);
            }catch(Exception e) {
                System.out.println(e);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> enviaAbort(Msg m,Address ad) {

        return enviaMensagem(m,"abortCoordenador",ad);
    }

    private void passouTempoTransacao(TransactionID idT){
        System.out.println("Passou tempo da transação: " + idT);
        Transaction transaction = transacoes.get(idT);

        if(transaction == null){
            System.out.println("Transacao não existe no mapa! Estranho!");
            return;
        }

        if(transaction.resultado.equals("I")) {
            System.out.println("Vou abortar a transação: " + idT);
            //posso cancelar se o resultado ainda for P
            writerLog.append(new LogEntry(idT,"A",null));
            transaction.resultado = "A";
            System.out.println("Res: " + transacoes.get(idT).resultado);
            Msg paraMandarAux = new Msg(idT,null);
            //completar todos os locks (pois a resposta será abort)
            for(Participante p: transaction.locksObtidos){
                p.espera.complete(true);
            } //depois podemos por um allOf antes do abort
            enviaAbort(paraMandarAux,new ArrayList<>(transaction.participantes.keySet()),transaction);

            es.schedule(() -> {
                cicloTerminar(idT);
            }, DELTA, TimeUnit.SECONDS);
        }

        //se n for P n é preciso fazer mais nada, pois já foi feito ao alterar o resultado

    }

    private void analisaTransacaoControlador(){

        for(Transaction t: transacoes.values()){
            if(t.resultado.equals("I")){
                //mandar mensagem para todos prepared
                Msg paraMandar = new Msg(t.xid,null);
                enviaPrepared(paraMandar,t);


                //dar tempo para a resposta
                es.schedule( ()-> {
                    passouTempoTransacao(t.xid);

                }, DELTA, TimeUnit.SECONDS);
            }
            else{
                if(t.resultado.equals("C")){
                    //mandar mensagem para todos commit
                    System.out.println("Transacao efetuada com sucesso!");

                    //mandar mensagem a todos a dizer commit
                    MsgCommit msgC = new MsgCommit(t.xid,null);
                    enviaCommit(msgC,t);


                    es.schedule( ()-> {
                        cicloTerminar(t.xid);
                    }, DELTA, TimeUnit.SECONDS);

                }
                else{
                    if(t.resultado.equals("A")){
                        //mandar mesnsagem para todos abort
                        Msg paraMandar = new Msg(t.xid,null);

                        enviaAbort(paraMandar,new ArrayList<>(t.participantes.keySet()),t);


                        es.schedule( ()-> {
                            cicloTerminar(t.xid);
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
            if(e.xid.xid > xid){
                //ele só mantém registo das suas transacoes
                xid = e.xid.xid;
            }
            //verifica se a transacao já existe no mapa
            Transaction t = transacoes.get(e.xid);
            if(t == null) {
                //caso não exista cria e adiciona ao mapa
                HashMap<Address, Object> part = null;
                //System.out.println(e.toString());
                if (e.valores != null) {
                    //se existirem valores (em caso de I ou C) entam vai buscar os participantes envolvidos e cada valor
                    // para o participante
                    part = controla.distribuiPorParticipante(e.valores);//distribuiPorParticipante.apply(e.valores);
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


    public TwoPCControlador(Address[] e, Address id, ManagedMessagingService ms,
                            Serializer ser, InterfaceControlador ic){
        super(e,id,ms,ser);

        log = SegmentedJournal.builder()
                .withName("exemploIDCoordenador" + this.meuEnd)
                .withSerializer(s)
                .build();

        readerLog = log.openReader(0);
        writerLog = log.writer();

/*        this.distribuiPorParticipante = dPP;
        this.juntaValores = jV;
*/
        this.controla = ic;

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
                    System.out.println("Recebi ok para uma transação que já se encontra terminada!");
                    return;
                }

                if(transacoes.get(nova.id).resultado.equals("I")){
                    System.out.println("Conflito no OK");
                }
                else{
                    //ainda não existe resultado
                    Transaction t = transacoes.get(nova.id);

                    if(t == null){
                        System.out.println("Transação não existe ao receber um ok!");
                        return;
                    }

                    t.quaisResponderam.add(o);


                    if(t.quaisResponderam.size() == t.participantes.size()){
                        //já responderam todos e pode-se mandar fazer commit
                        writerLog.append(new LogEntry(nova.id, "F", null));
                        boolean resPedido = t.resultado.equals("C");
                        t.resultado = "F";
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

                if(taux == null){
                    System.out.println("Não existe transacao ao receber prepared!");
                    return;
                }

                //temos depois de escrever no log o lock
                adicionaLock(o,taux);

                //pensar depois para o caso em que a transacao está terminada

                if(!taux.resultado.equals("I")){
                    //já existe resultado diferente de I e então pode-se mandar mensagem consoante esse resultado
                    System.out.println("Recebi prepared e já existe resultado!");
                    if(!taux.resultado.equals("F")) {
                        //e o resultado n é F, se fosse F era estranho (à partida não pode acontecer)
                        if (taux.resultado.equals("A")) {
                            Msg paraMandar = new Msg(nova.id,taux.participantes.get(o));
                            enviaAbort(paraMandar, o);
                        }

                        else{
                            MsgCommit paraMandar = new MsgCommit(nova.id,taux.participantes.get(o));
                            enviaCommit(paraMandar,o);
                        }
                    }
                }

                else{
                    //ainda não existe resultado
                    System.out.println("Recebi prepared e ainda não existe resultado!");
                    //ainda não existe resultado
                    taux.quaisResponderam.add(o);
                    System.out.println("Adicionei responderam");

                    if(taux.quaisResponderam.size() == taux.participantes.size()){
                        System.out.println("Já responderam todos!");
                        //já responderam todos e pode-se mandar fazer commit
                        Object valoresGuardar = controla.juntaValores(taux.participantes);//juntaValores.apply(taux.participantes);
                        writerLog.append(new LogEntry(nova.id, "C", valoresGuardar));
                        taux.resultado = "C";
                        taux.quaisResponderam = new HashSet<>(); //volta-se a por que ninguem respondeu
                        // (pois ainda nao responderam ao commit)
                        System.out.println("Transacao efetuada com sucesso!");

                        //mandar mensagem a todos a dizer commit
                        MsgCommit msgC = new MsgCommit(nova.id, null);
                        enviaCommit(msgC,taux);

                        System.out.println("Resultado depois: " + transacoes.get(nova.id).resultado);

                        es.schedule( ()-> {
                            cicloTerminar(taux.xid);
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

            Transaction transaction = transacoes.get(nova.id);

            if(transaction == null){
                System.out.println("Transacao nao existe ao receber abort");
                return;
            }

            if(transaction.resultado.equals("A")){
                //já tem resultado, pelo que já mandou para todos e só mandamos para o que enviou
                Msg paraMandar = new Msg(nova.id,transaction.participantes.get(a));
                enviaAbort(paraMandar,a);
            }
            else {
                if (transaction.resultado.equals("C") || transaction.resultado.equals("F")) {
                    //conflito nos resultados, recebi um abort quando já tenho um resultado C ou F
                    // em nenhum dos casos devia receber abort e à partida não acontece
                    System.out.println("Conflito nos resultados no controlador ao receber abort!");
                }
                else {
                    //pode-se por o resultado a A, pois um abortou
                    writerLog.append(new LogEntry(nova.id, "A", null));


                    transaction.resultado = "A";
                    System.out.println("Transacao abortada com sucesso!");


                    //mandar mensagem para todos os participantes daquela transacao
                    Msg paraMandar = new Msg(nova.id,null);

                    //completar todos os locks (pois a resposta será abort)
                    for(Participante p: transaction.locksObtidos){
                        p.espera.complete(null);
                    } //depois podemos por um allOf antes do abort

                    enviaAbort(paraMandar, new ArrayList<>(transaction.participantes.keySet()),transaction);


                    es.schedule(() -> {
                        cicloTerminar(transaction.xid);
                    }, DELTA, TimeUnit.SECONDS);


                }
            }
        }, es);

        ms.registerHandler("put",(a,m) -> {

            System.out.println("Recebi put");

            PedidoPut pp = s.decode(m);

            PedidoID pi = new PedidoID(a,pp.id);



            for(Map.Entry<PedidoID,TransactionID> mePi : pedidos.entrySet()){

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

                //HashMap<Long, byte[]> valores = new HashMap<>(pp.valores);
                TransactionID newXid = new TransactionID(++xid,meuEnd.toString());

                writerLog.append(new LogEntry(newXid, "I", pp.valores, pi));
                pedidos.put(pi, newXid);

                System.out.println("Vou mandar!");

                //mandar mensagem para todos para iniciar transacao
                Msg paraMandar = new Msg(newXid,null);
                HashMap<Address, Object> participantes = controla.distribuiPorParticipante(pp.valores);//distribuiPorParticipante.apply(pp.valores);
                Transaction novaTransacao = new Transaction(newXid, "I", participantes,pi);
                novaTransacao.terminada = new CompletableFuture<Boolean>();
                transacoes.put(newXid, novaTransacao);

                enviaPrepared(paraMandar, novaTransacao); //depois aqui vai ter thenQQCoisa



                es.schedule(() -> {
                    try{
                        System.out.println("Passou tempo");
                        System.out.println("Esta terminado o tempo de: " + paraMandar.id);
                    }
                    catch(Exception exc){
                        System.out.println(exc);
                    }
                    passouTempoTransacao(novaTransacao.xid);
                }, TEMPOTRANSACAO, TimeUnit.SECONDS);

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

    private void cicloTerminar(TransactionID idT){
        System.out.println("Tentar finalizar transação: " + idT);
        Transaction transaction = transacoes.get(idT);

        if(transaction == null){
            System.out.println("Transação não existe! Ver o que se passa!");
            return;
        }

        if(transaction.resultado.equals("F")) {
            //se resultado for F é porque se pode "esquecer" a transação
            System.out.println("Esquecer a Transacao!");
        }
        else{
            //senao é pq ainda não responderam todos ao A/C
            ArrayList<Address> naoResponderam = new ArrayList<>(transaction.participantes.keySet());
            naoResponderam.removeAll(transaction.quaisResponderam);

            if(transaction.resultado.equals("A")){
                //se for A, manda-se abort
                Msg msg = new Msg(transaction.xid,null);
                enviaAbort(msg,naoResponderam,transaction);

            }
            else{
                //senao e C e manda-se commit
                MsgCommit msg = new MsgCommit(transaction.xid, null);
                enviaCommit(msg,transaction);

            }

            es.schedule( ()-> {
                cicloTerminar(idT);
            }, DELTA, TimeUnit.SECONDS);
        }

    }

    public HashMap<Address,HashMap<Long,byte[]>> participantesEnvolvidos(HashMap<Long, byte[]> valores){
        HashMap<Address,HashMap<Long,byte[]>> participantes = new HashMap<>();
        for(Long aux : valores.keySet()){
            int resto = (int)(aux % (end.length)); //para já o coordenador n participa
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
