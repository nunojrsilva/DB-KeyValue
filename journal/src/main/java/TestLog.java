import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

//mensagem para ser enviada!
class Msg {
    int id;

    public Msg(int id) {
        this.id = id;
    }
}

//mensagem para ser enviado do coordenador ao participante para o commit
class MsgCommit extends Msg {
    HashMap<Long,byte[]> valores;

    public MsgCommit(int id, HashMap<Long, byte[]> valores) {
        super(id);
        this.valores = valores;
    }
}


class LogEntry {
    public int xid;
    public String data;
    public HashMap<Long,byte[]> valores;

    public LogEntry() {}
    public LogEntry(int xid, String data, HashMap<Long, byte[]> valores) {
        this.xid=xid;
        this.data=data;
        this.valores = valores;
    }

    @Override
    public String toString() {
        String mapa = null;
        if(valores != null){
            mapa = "{";

            for(Map.Entry<Long,byte[]> aux: valores.entrySet()){

                mapa += aux.getKey() + "=" + new String(aux.getValue(), StandardCharsets.UTF_8) + ",";
            }
            mapa += "}";
        }
        return "LogEntry{" +
                "xid=" + xid +
                ", data='" + data + '\'' +
                ", valores=" + mapa +
                '}';
    }
}

class Transaction{
    public int xid;
    public String resultado; //pode ser I; A ou C; F para o controlador
                            //pode ser P ou A; A ou C para o participante

    //Estas variaveis são para o Coodenador ... Criar uma class com extends?
    public HashSet<Address> quaisResponderam = new HashSet<>();
    public HashMap<Address, HashMap<Long, byte[]>> participantes;
    //so podemos fazer commit quando o tamanho dos dois maps forem iguais!
    public CompletableFuture<Boolean> terminada;

    public Transaction(int xid, String resultado){
        this.xid = xid;
        this.resultado = resultado;
    }

    public Transaction(int xid, String resultado, HashMap<Address, HashMap<Long, byte[]>> participantes) {
        this.xid = xid;
        this.resultado = resultado;
        this.participantes = participantes;
    }
}

class TwoPCParticipante {

    //private HashMap<Integer, ArrayList<LogEntry>> transacoesANTIGA = new HashMap<>();
    private SegmentedJournal<Object> log;
    private SegmentedJournalReader<Object> readerLog;
    private SegmentedJournalWriter<Object> writerLog;

    private HashMap<Integer, Transaction> transacoes = new HashMap<>();

    /**
     * @valores Para guadar os pares chave-valor
     */
    private HashMap<Long, byte[]> valores = new HashMap<>();

    private Address[] end;
    private int meuID;
    private ManagedMessagingService ms;
    private Serializer s;


    /**
     * ATENCAO!!!
     * Para nao dar confusao, eu mudei o nome de transacoes para transacoesANTIGA na variavel! Esta a cima em comentarios
     */
    private void analisaTransacaoParticipante() {

        //Resultados possiveis :
        for (Transaction t : transacoes.values()) {
            Msg paraMandar = new Msg(t.xid);
            switch (t.resultado) {
                case ("P"):
                    ms.sendAsync(end[0], "prepared", s.encode(paraMandar));
                    break;
                case ("A"):
                    ms.sendAsync(end[0], "abort", s.encode(paraMandar));
                    break;

                case ("C"):
                    ms.sendAsync(end[0], "commit", s.encode(paraMandar));
                    break;


            }

        }

    }


    public void recuperaLogParticipante(){
        System.out.println("Recupera Part");

        while(readerLog.hasNext()) {

            // Leitura do log
            LogEntry e = (LogEntry) readerLog.next().entry();

            System.out.println(e.toString());

            Transaction t = transacoes.get(e.xid);

            if (t == null) {

                if (e.valores != null)
                    valores = e.valores;


                t = new Transaction(e.xid, e.data);
                transacoes.put(e.xid, t);



            }

            else {
                //Altera o resultado
                t.resultado = e.data;
                if (e.valores != null)
                    valores = e.valores;

            }

        }

        analisaTransacaoParticipante();

    }





    public TwoPCParticipante(Address[] e, int id){

        s = Serializer.builder()
                .withTypes(Msg.class)
                .withTypes(LogEntry.class)
                .withTypes(MsgCommit.class)
                .build();

        log = SegmentedJournal.builder()
                .withName("exemploID" + id)
                .withSerializer(s)
                .build();

        ExecutorService es = Executors.newSingleThreadExecutor();

        meuID = id;
        int k = 0;
        end = new Address[e.length];
        System.out.println("TamE: " + e.length);
        for(Address aux: e){
            end[k++] = aux;
        }

        System.out.println("TamEnd: " + end.length);

        ms = NettyMessagingService.builder()
                .withAddress(end[meuID])
                .build();
        ms.start();

        readerLog = log.openReader(0);
        writerLog = log.writer();
        if(id==0){
            System.out.println("Erro!");
            return;
        }
        else{
            //recuperaLogParticipante();
        }


        ms.registerHandler("prepared", (a,m)-> {
            Msg nova = s.decode(m);

            /**
             * Vou verificar primeiro se já recebi uma resposta para este notificação, se sim tenho de dar a mesma
             */
            if(transacoes.containsKey(nova.id)){
                Msg paraMandar = new Msg(nova.id);

                //Para já vou deixar end[0], mas depois o coordenador poderá ser outro ...
                String resultado = transacoes.get(nova.id).resultado;
                switch (resultado){
                    case "P": ms.sendAsync(end[0], "prepared", s.encode(paraMandar)); break;
                    case "A": ms.sendAsync(end[0], "abort", s.encode(paraMandar)); break;
                }
                return;
            }


            String assunto = "prepared";
            int res = 1;

            if(res == 0){
                assunto = "abort";
                /**
                 * Vou guardar os valores no Log, caso haja um abort!
                 */
                writerLog.append(new LogEntry(nova.id,"A", null));
                Transaction t = new Transaction(nova.id, "A");
                transacoes.put(nova.id, t);
            }
            else{
                /**
                 * Vou guardar a minha decisão no Log "P" <- Estou pronto para iniciar
                 * Vou criar uma nova entrada na transacao
                 */
                writerLog.append(new LogEntry(nova.id,"P", null));
                Transaction t = new Transaction(nova.id, "P");
                transacoes.put(nova.id, t);
            }

            /**
             * Vou agora mandar a resposta para o coordenador
             * ----> Não usei a mesma mensagem, porque posteriormente pode nos dar mais jeito para trabalhar com os varios coordenadores
             * Mais uma vez tou a utilizar o end[0] que posteriormente poderá ser mudado
             */
            Msg paraMandar = new Msg(nova.id);
            ms.sendAsync(end[0], assunto, s.encode(paraMandar));

        }, es);

        ms.registerHandler("abort", (a,m)-> {
            Msg nova = s.decode(m);

            if(transacoes.containsKey(nova.id) == false){
                System.out.println("Não foi possivel abortar a transacao ..." + nova.id);
            }
            else{
                /**
                 * Para o abort se ja tiver um abort ignoramos, senão tenho de guardar o resultado
                 */
                if(transacoes.get(nova.id).resultado.equals("A")){
                    System.out.println("Já recebi uma mensagem de abort para a transacao " + nova.id);
                }else {
                    System.out.println("Vou abortar a transacao " + nova.id);
                    writerLog.append(new LogEntry(nova.id, "A", null));
                    Transaction t = transacoes.get(nova.id);
                    t.resultado = "F";
                    transacoes.put(nova.id, t);
                }
            }

            Msg paraMandar = new Msg(nova.id);
            ms.sendAsync(end[0], "ok", s.encode(paraMandar));

        }, es);

        ms.registerHandler("commit", (a,m)-> {
            MsgCommit nova = s.decode(m);

            if(transacoes.containsKey(nova.id) == false){
                System.out.println("Não foi possivel efetuar commit da transacao ... " + nova.id);
            }
            else{
                /**
                 * Primeiro verifico se já não guardei a
                 */
                if(transacoes.get(nova.id).resultado.equals("C")){
                    System.out.println("Já tinha os valores referentes à transacao " + nova.id);
                }else {
                    /**
                     * Tarefas:
                     * --> Guardar o resultado no log
                     * --> Guardar os valores
                     * --> Adicionar a transacao no hashmap
                     */
                    System.out.println("Vou guardar os valores para a transacao " + nova.id);
                    writerLog.append(new LogEntry(nova.id, "C", nova.valores));
                    for(Map.Entry<Long, byte[]> entry: nova.valores.entrySet()){
                        /**
                         * Senão tiver a key, entao devo de adicionar tudo
                         * Se ja tiver tenho de adicionar os novos valores de bytes ao array e ainda atualizar a transaction
                         */
                        Long key = entry.getKey();
                        byte[] value = entry.getValue();
                        valores.put(key, value);
                    }

                    Transaction t = transacoes.get(nova.id);
                    t.resultado = "C";
                    transacoes.put(nova.id, t);
                }
            }

            Msg paraMandar = new Msg(nova.id);
            ms.sendAsync(end[0], "ok", s.encode(paraMandar));

        }, es);
    }
}

class TwoPCControlador{

    private final int DELTA = 10;
    private ArrayList<Integer> paraCancelar = new ArrayList<>(); //ids com as proximas a serem canceladas
    private ArrayList<Boolean> possoCancelar = new ArrayList<>(); //valores bools que indicam se podemos ou não apagar
    private ArrayList<Integer> paraTerminar = new ArrayList<>(); //ids com as proximas a serem terminadas
    private ArrayList<Boolean> possoTerminar = new ArrayList<>(); //valores bools que indicam se podemos ou não terminar

    //private HashMap<Integer, ArrayList<LogEntry>> transacoesLog = new HashMap<>(); //para guardar as transacoes
    // dos logs

    private SegmentedJournal<Object> log;
    private SegmentedJournalReader<Object> readerLog;
    private SegmentedJournalWriter<Object> writerLog;


    private HashMap<Integer, Transaction> transacoes = new HashMap<>();

    private Address[] end;
    private int meuID;
    private ManagedMessagingService ms;
    private Serializer s;
    private int xid;
    private ScheduledExecutorService es;
    //private Consumer<Msg> handlerMensagem;

    private CompletableFuture<Void> enviaCommit(MsgCommit mc, List<Address> part, Transaction t){

        if( part.size() == 0 ) {
            return CompletableFuture.completedFuture(null);
        }
        else {
            Address ad = part.remove(0);
            mc.valores = t.participantes.get(ad);
            return ms.sendAsync(ad, "commit", s.encode(mc))
                    .thenCompose(aux -> {
                        return enviaCommit(mc,part,t);
                    });
        }
    }

    private CompletableFuture<Void> enviaCommit(MsgCommit mc,Address ad) {

        return ms.sendAsync(ad, "commit", s.encode(mc))
            .thenCompose(aux -> {
                    return CompletableFuture.completedFuture(null);
        });
    }

    private CompletableFuture<Void> enviaPrepared(Msg m, List<Address> part){

        if( part.size() == 0 ) {
            return CompletableFuture.completedFuture(null);
        }
        else {
            Address ad = part.remove(0);
            return ms.sendAsync(ad, "prepared", s.encode(m))
                    .thenCompose(aux -> {
                        return enviaPrepared(m,part);
                    });
        }
    }

    private CompletableFuture<Void> enviaAbort(Msg m, List<Address> part){

        if( part.size() == 0 ) {
            return CompletableFuture.completedFuture(null);
        }
        else {
            Address ad = part.remove(0);
            return ms.sendAsync(ad, "abort", s.encode(m))
                    .thenCompose(aux -> {
                        return enviaPrepared(m,part);
                    });
        }
    }

    private CompletableFuture<Void> enviaAbort(Msg m,Address ad) {

        return ms.sendAsync(ad, "abort", s.encode(m))
            .thenCompose(aux -> {
                    return CompletableFuture.completedFuture(null);
        });
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
            try {
                enviaAbort(paraMandarAux,new ArrayList<>(transacoes.get(idT).participantes.keySet())).get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }

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
                try {
                    enviaPrepared(paraMandar,new ArrayList<>(t.participantes.keySet())).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }

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
                    try {
                        enviaCommit(msgC,new ArrayList<>(t.participantes.keySet()),t).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }

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
                        try {
                            enviaAbort(paraMandar,new ArrayList<>(t.participantes.keySet())).get();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }

                        es.schedule( ()-> {
                            cicloTerminar();
                        }, DELTA, TimeUnit.SECONDS);

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

                t = new Transaction(e.xid, e.data, part);
                transacoes.put(e.xid, t);
            }
            else{
                //caso exista apenas altera o resultado
                t.resultado = e.data;
            }
        }

        analisaTransacaoControlador();

    }


    public TwoPCControlador(Address[] e, int id){

        s = Serializer.builder()
                .withTypes(Msg.class)
                .withTypes(LogEntry.class)
                .withTypes(MsgCommit.class)
                .build();

        log = SegmentedJournal.builder()
                .withName("exemploID" + id)
                .withSerializer(s)
                .build();

        es = Executors.newSingleThreadScheduledExecutor();

        meuID = id;
        int k = 0;
        end = new Address[e.length];
        System.out.println("TamE: " + e.length);
        for(Address aux: e){
            end[k++] = aux;
        }

        System.out.println("TamEnd: " + end.length);

        ms = NettyMessagingService.builder()
                .withAddress(end[meuID])
                .build();
        ms.start();

        readerLog = log.openReader(0);
        writerLog = log.writer();

        if(id == 0){
            recuperaLogControlador();
        }
        else{
            System.out.println("Erro!");
            return;
        }

        System.out.println("Passei recupera log!");

        System.out.println("Sou o controlador!");
        //controlador tem de registar handler para ao receber um Prepared ou Abort

        ms.registerHandler("ok", (o,m) -> {
            System.out.println("Recebi um ok! " + o);
            try {
                Msg nova = s.decode(m);

                //pensar depois para o caso em que a transacao está terminada

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
                        t.resultado = "F";
                        int indiceAux = paraTerminar.indexOf(Integer.valueOf(nova.id));
                        possoTerminar.set(indiceAux, true);
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

                //pensar depois para o caso em que a transacao está terminada
                System.out.println("Passei decode!");
                if(!taux.resultado.equals("I")){
                    //já existe resultado diferente de I e então pode-se mandar mensagem consoante esse resultado

                    if(!taux.resultado.equals("F")) {
                        //e o resultado n é F
                        if (taux.resultado.equals("A")) {
                            Msg paraMandar = new Msg(nova.id);
                            enviaAbort(paraMandar, o).get();
                        }

                        else{
                            MsgCommit paraMandar = new MsgCommit(nova.id,taux.participantes.get(o));
                            enviaCommit(paraMandar,o).get();
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
                        System.out.println("Resultado depois: " + transacoes.get(nova.id).resultado);
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
                        enviaCommit(msgC,new ArrayList<>(t.participantes.keySet()),t).get();

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
            Msg nova = s.decode(m);
            System.out.println("Recebi abort: " + nova.id + "!" + a);

            if(transacoes.get(nova.id).resultado.equals("A")){
                //já tem resultado, pelo que já mandou para todos e só mandamos para o que enviou
                Msg paraMandar = new Msg(nova.id);
                try {
                    enviaAbort(paraMandar,a).get();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (ExecutionException e1) {
                    e1.printStackTrace();
                }
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
                    try {
                        enviaAbort(paraMandar, new ArrayList<>(transacoes.get(nova.id).participantes.keySet())).get();
                    } catch (InterruptedException e1) {
                        e1.printStackTrace();
                    } catch (ExecutionException e1) {
                        e1.printStackTrace();
                    }

                    es.schedule(() -> {
                        cicloTerminar();
                    }, DELTA, TimeUnit.SECONDS);


                }
            }
        }, es);
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
                try {
                    enviaAbort(msg,naoResponderam).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }else{
                MsgCommit msg = new MsgCommit(t.xid, null);
                try {
                    enviaCommit(msg,naoResponderam,t).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
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

    public void iniciaTransacao(HashMap<Long,byte[]> valores){
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
            e.printStackTrace();
        }

        paraCancelar.add(paraMandar.id); //adicionar este id ao array para cancelar a transacao com o id
        possoCancelar.add(true); //para já podemos cancelar (até que alguem ponha resposta)

            es.schedule( ()-> {
                passouTempoTransacao();
            }, DELTA, TimeUnit.SECONDS);

    }
}

public class TestLog {
    public static void leLog(SegmentedJournal<Object> log){
        SegmentedJournalReader<Object> readLog = log.openReader(0);
        while(readLog.hasNext()){
            LogEntry e = (LogEntry) readLog.next().entry();
            System.out.println(e);
        }
    }

    public static void leLogs(Address e[]){
        Serializer ser = Serializer.builder()
                .withTypes(Msg.class)
                .withTypes(LogEntry.class)
                .withTypes(MsgCommit.class)
                .build();


        for(int i = 0; i < e.length; i++){
            SegmentedJournal<Object> log = SegmentedJournal.builder()
                    .withName("exemploID" + i)
                    .withSerializer(ser)
                    .build();
            System.out.println("-------------------------------ID:"+i+"-----------------------------");
            leLog(log);
        }
    }
    public static void main(String[] args) {

        Address[] end = {
                Address.from("localhost:23451"),
                Address.from("localhost:23452"),
                Address.from("localhost:23453"),
                //Address.from("localhost:23454"),
                //Address.from("localhost:23455"),
        };

        int id = Integer.parseInt(args[0]);
        if(id==-1){
            leLogs(end);
        }
        else {
            if (id == 0) {
                TwoPCControlador tpc = new TwoPCControlador(end, id);
                HashMap<Long, byte[]> valores = new HashMap<>();
                valores.put(Long.valueOf(1), "ola".getBytes());
                valores.put(Long.valueOf(2), "cenas".getBytes());
                valores.put(Long.valueOf(3), "muitascoisas".getBytes());
                tpc.iniciaTransacao(valores);
            } else {
                TwoPCParticipante tpp = new TwoPCParticipante(end, id);
            }
        }

    }
}
