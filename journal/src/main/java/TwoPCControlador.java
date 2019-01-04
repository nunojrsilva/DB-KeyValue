
import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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


    public HashMap<TransactionID, Transaction> transacoes = new HashMap<>();
    public HashMap<PedidoID,TransactionID> pedidos = new HashMap<>();

    private int xid;

    private InterfaceControlador controla;

    private void adicionaLock(Address a, Transaction t){

        for(Participante p: t.locksObtidos){
            if(p.endereco.toString().equals(a.toString())){
                p.espera.complete(true);
                return;
            }
        }
    }

    private CompletableFuture<Void> enviaMensagem(Msg m, String assunto, Address a){

        //return CompletableFuture.allOf(esperar).thenAccept(v -> {
        try{
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
                    mc.valores = t.participantes.get(p.endereco);
                    enviaMensagem(mc,"commitCoordenador",p.endereco);
                }
                else{
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
                    mc.valores = t.participantes.get(p.endereco);
                    enviaMensagem(mc,"preparedCoordenador",p.endereco);
                }
                else{
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

        Transaction transaction = transacoes.get(idT);

        if(transaction == null){
            System.out.println("Transacao não existe no mapa! Estranho!");
            return;
        }

        if(transaction.resultado.equals("I")) {
            //posso cancelar se o resultado ainda for P
            writerLog.append(new LogEntry(idT,"A",null));
            transaction.resultado = "A";
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

    public CompletableFuture<Boolean> iniciaTransacao(Address a, String ppid, Object ppvalores){

        //PedidoPut pp = s.decode(m);

        PedidoID pi = new PedidoID(a, ppid);

        //HashMap<Long, byte[]> valores = new HashMap<>(pp.valores);
        TransactionID newXid = new TransactionID(++xid, meuEnd.toString());

        writerLog.append(new LogEntry(newXid, "I", ppvalores, pi));
        pedidos.put(pi, newXid);


        //mandar mensagem para todos para iniciar transacao
        Msg paraMandar = new Msg(newXid, null);
        HashMap<Address, Object> participantes = controla.distribuiPorParticipante(ppvalores);//distribuiPorParticipante.apply(pp.valores);
        Transaction novaTransacao = new Transaction(newXid, "I", participantes, pi);
        novaTransacao.terminada = new CompletableFuture<Boolean>();
        transacoes.put(newXid, novaTransacao);

        enviaPrepared(paraMandar, novaTransacao); //depois aqui vai ter thenQQCoisa


        es.schedule(() -> {
            passouTempoTransacao(novaTransacao.xid);
        }, TEMPOTRANSACAO, TimeUnit.SECONDS);


        return novaTransacao.terminada;
    }



    public TwoPCControlador(Address[] e, Address id, ManagedMessagingService ms,
                            Serializer ser, InterfaceControlador ic, ScheduledExecutorService ses) {
        super(e, id, ms, ser, ses);

        log = SegmentedJournal.builder()
                .withName("exemploIDCoordenador" + this.meuEnd)
                .withSerializer(s)
                .build();

        readerLog = log.openReader(0);
        writerLog = log.writer();

        this.controla = ic;

        recuperaLogControlador();

        System.out.println("Passei recupera log!");

        System.out.println("Sou o controlador!");
        //controlador tem de registar handler para ao receber um Prepared ou Abort

        ms.registerHandler("ok", (o, m) -> {
            try {
                Msg nova = s.decode(m);

                //pensar depois para o caso em que a transacao está terminada
                if (transacoes.get(nova.id).resultado.equals("F")) {
                    return;
                }

                if (transacoes.get(nova.id).resultado.equals("I")) {
                    System.out.println("Conflito no OK");
                } else {
                    //ainda não existe resultado
                    Transaction t = transacoes.get(nova.id);

                    if (t == null) {
                        System.out.println("Transação não existe ao receber um ok!");
                        return;
                    }

                    t.quaisResponderam.add(o);


                    if (t.quaisResponderam.size() == t.participantes.size()) {
                        //já responderam todos e pode-se mandar fazer commit
                        writerLog.append(new LogEntry(nova.id, "F", null));
                        boolean resPedido = t.resultado.equals("C");
                        t.resultado = "F";
                        t.terminada.complete(resPedido);
                        System.out.println("completa a transacao!");

                    }
                }
            } catch (Exception exc) {
                System.out.println("exc: " + exc);
            }
        }, es);

        ms.registerHandler("prepared", (o, m) -> {

            try {
                Msg nova = s.decode(m);
                Transaction taux = transacoes.get(nova.id);

                if (taux == null) {
                    System.out.println("Não existe transacao ao receber prepared!");
                    return;
                }

                //temos depois de escrever no log o lock
                adicionaLock(o, taux);

                //pensar depois para o caso em que a transacao está terminada

                if (!taux.resultado.equals("I")) {
                    //já existe resultado diferente de I e então pode-se mandar mensagem consoante esse resultado
                    if (!taux.resultado.equals("F")) {
                        //e o resultado n é F, se fosse F era estranho (à partida não pode acontecer)
                        if (taux.resultado.equals("A")) {
                            Msg paraMandar = new Msg(nova.id, taux.participantes.get(o));
                            enviaAbort(paraMandar, o);
                        } else {
                            MsgCommit paraMandar = new MsgCommit(nova.id, taux.participantes.get(o));
                            enviaCommit(paraMandar, o);
                        }
                    }
                } else {
                    //ainda não existe resultado
                    taux.quaisResponderam.add(o);

                    if (taux.quaisResponderam.size() == taux.participantes.size()) {
                        //já responderam todos e pode-se mandar fazer commit
                        Object valoresGuardar = controla.juntaValores(taux.participantes);//juntaValores.apply(taux.participantes);
                        writerLog.append(new LogEntry(nova.id, "C", valoresGuardar));
                        taux.resultado = "C";
                        taux.quaisResponderam = new HashSet<>(); //volta-se a por que ninguem respondeu
                        // (pois ainda nao responderam ao commit)

                        //mandar mensagem a todos a dizer commit
                        MsgCommit msgC = new MsgCommit(nova.id, null);
                        enviaCommit(msgC, taux);

                        es.schedule(() -> {
                            cicloTerminar(taux.xid);
                        }, DELTA, TimeUnit.SECONDS);

                    }
                }
            } catch (Exception exc) {
                System.out.println("exc: " + exc);
            }
        }, es);

        ms.registerHandler("abort", (a, m) -> {
            Msg nova = s.decode(m);

            Transaction transaction = transacoes.get(nova.id);

            if (transaction == null) {
                System.out.println("Transacao nao existe ao receber abort");
                return;
            }

            if (transaction.resultado.equals("A")) {
                //já tem resultado, pelo que já mandou para todos e só mandamos para o que enviou
                Msg paraMandar = new Msg(nova.id, transaction.participantes.get(a));
                enviaAbort(paraMandar, a);
            } else {
                if (transaction.resultado.equals("C") || transaction.resultado.equals("F")) {
                    //conflito nos resultados, recebi um abort quando já tenho um resultado C ou F
                    // em nenhum dos casos devia receber abort e à partida não acontece
                    System.out.println("Conflito nos resultados no controlador ao receber abort!");
                } else {
                    //pode-se por o resultado a A, pois um abortou
                    writerLog.append(new LogEntry(nova.id, "A", null));


                    transaction.resultado = "A";

                    //mandar mensagem para todos os participantes daquela transacao
                    Msg paraMandar = new Msg(nova.id, null);

                    //completar todos os locks (pois a resposta será abort)
                    for (Participante p : transaction.locksObtidos) {
                        p.espera.complete(null);
                    } //depois podemos por um allOf antes do abort

                    enviaAbort(paraMandar, new ArrayList<>(transaction.participantes.keySet()), transaction);


                    es.schedule(() -> {
                        cicloTerminar(transaction.xid);
                    }, DELTA, TimeUnit.SECONDS);


                }
            }
        }, es);


    }


    private void cicloTerminar(TransactionID idT){
        Transaction transaction = transacoes.get(idT);

        if(transaction == null){
            System.out.println("Transação não existe! Ver o que se passa!");
            return;
        }

        if(transaction.resultado.equals("F")) {
            //se resultado for F é porque se pode "esquecer" a transação

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
}
