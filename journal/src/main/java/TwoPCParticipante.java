import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiFunction;
import java.util.function.Function;

class Lock{
    TransactionID xid;
    int lockID;
    CompletableFuture<Void> obtido = new CompletableFuture<>();

    public Lock(TransactionID xid, int id) {
        this.xid = xid;
        lockID = id;
    }

    public boolean equals(Object o){
        if(o == null || !(o instanceof Lock)){
            return false;
        }

        Lock al = (Lock)o;

        return this.xid.equals(al.xid);
    }
}

class TwoPCParticipante extends TwoPC{

    private HashMap<TransactionID, Transaction> transacoes = new HashMap<>();

    /**
     * @valores Para guadar os pares chave-valor
     */
    private Object valores; //podemos mudar para ser modular
    private BiFunction<Object,Object,Object> atualizaValores; //funcao que atualiza os valores

    private Function<Object,Object> devolveValores; //funcao que devolve valores consoante
    // a regra utilizada
    /**
     * Para o 2PL:
     * Uma variável que diz quem tem o lock atual
     * Uma lista de possiveis locks, já com uma ordem associada
     */
    private Lock lockAtual = null;
    private int lockID = 0;
    private TreeSet<Lock> filaLock = new TreeSet<Lock>((o1, o2) -> {

        Lock lock1 = (Lock) o1;
        Lock lock2 = (Lock) o2;

        if(lock1.lockID > lock2.lockID){
            return 1;
        }
        if(lock1.lockID < lock2.lockID){
            return -1;
        }
        return 0;

        //return lock1.coordenador.toString().compareTo(lock2.coordenador.toString());
    });

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

    private CompletableFuture<Void> enviaOk(Msg m, Address coord) {

        return enviaMensagem(m,"ok",coord);
    }

    private CompletableFuture<Void> enviaPrepared(Msg m, Address coord){

        return enviaMensagem(m,"prepared",coord);
    }

    private CompletableFuture<Void> enviaAbort(Msg m, Address coord){

        return enviaMensagem(m,"abort",coord);
    }

    private void libertaLock(){
        /**
         * Vou agora libertar o lock
         */
        lockAtual = filaLock.pollFirst();
        System.out.println("Lock libertado!");
        if(lockAtual != null){
            System.out.println("Ja libertei o lock e vou avisar o seguinte!");
            enviaPrepared(new Msg(lockAtual.xid), Address.from(lockAtual.xid.coordenador));
        }
    }

    /**
     * ATENCAO!!!
     * Para nao dar confusao, eu mudei o nome de transacoes para transacoesANTIGA na variavel! Esta a cima em comentarios
     */
    private void analisaTransacaoParticipante() {
        boolean primeiroLock = true;
        //Resultados possiveis :
        for (Transaction t : transacoes.values()) {
            Msg paraMandar = new Msg(t.xid);
            switch (t.resultado) {
                case ("P"):
                    /*Lock l;
                    if(lockAtual.xid.equals(t.xid)){
                        l = lockAtual;
                    }
                    else{
                        l = filaLock.stream().filter(aux -> aux.xid.equals(t.xid)).reduce(null,(a,b) -> b);
                    }

                    if(l == null){
                        System.out.println("Lock é null!");
                    }
                    */
                    Lock l = new Lock(t.xid,lockID++);
                    filaLock.add(l);
                    l.obtido.thenAccept(v -> {
                        System.out.println("Posso enviar!");
                        enviaPrepared(paraMandar, Address.from(paraMandar.id.coordenador));
                    });

                    break;
                case ("A"):
                    //PRECISO POR AQUI UM lockID++?????????
                    enviaAbort(paraMandar,Address.from(paraMandar.id.coordenador));
                    break;

                case ("C"):
                    enviaOk(paraMandar,Address.from(paraMandar.id.coordenador));
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

            /*if(e.data.equals("P")){
                Lock aux = new Lock(t.xid,lockID++);
                filaLock.add(aux);
            }
            else{
                System.out.println("Antes de remover: " + filaLock.size());
                filaLock.removeIf(lock -> (lock.xid.equals(e.xid)));
                System.out.println("Depois de remover: " + filaLock.size());
            }
            */

        }

        analisaTransacaoParticipante();

        lockAtual = filaLock.pollFirst();
        if(lockAtual != null) {
            System.out.println("Lock atual: " + lockAtual.xid);
            lockAtual.obtido.complete(null);
        }

    }





    public TwoPCParticipante(Address[] e, Address id, ManagedMessagingService ms, Object valores,
                             BiFunction<Object,Object,Object> atualizaValores){

        super(e,id,ms);

        log = SegmentedJournal.builder()
                .withName("exemploIDParticipante" + this.meuEnd)
                .withSerializer(s)
                .build();

        readerLog = log.openReader(0);
        writerLog = log.writer();

        this.valores = valores;
        this.atualizaValores = atualizaValores;


        System.out.println("TamEnd: " + end.length);

        recuperaLogParticipante();

        ms.registerHandler("preparedCoordenador", (a,m)-> {
            System.out.println("Recebi prepared!");
            Msg nova = s.decode(m);

            /**
             * Vou verificar primeiro se já recebi uma resposta para este notificação, se sim tenho de dar a mesma
             */
            if(transacoes.containsKey(nova.id) &&
                    !transacoes.get(nova.id).resultado.equals("P")){
                System.out.println("Ja tenho esta transacao!Decidir o que fazer com o lock!");
                Msg paraMandar = new Msg(nova.id);

                //Para já vou deixar end[0], mas depois o coordenador poderá ser outro ...
                String resultado = transacoes.get(nova.id).resultado;
                switch (resultado){
                    case "A":
                        enviaAbort(paraMandar,Address.from(paraMandar.id.coordenador));
                        break;
                }
                return;
            }



            String assunto = "prepared";

            /**
             * Vou guardar a minha decisão no Log "P" <- Estou pronto para iniciar
             * Vou criar uma nova entrada na transacao
             */
            if(!transacoes.containsKey(nova.id)){
                writerLog.append(new LogEntry(nova.id, "P", null));

                Transaction t = new Transaction(nova.id, "P");
                transacoes.put(nova.id, t);

            }

            Lock meuLock = new Lock(nova.id,lockID++);

            if(lockAtual == null){
                //posso mandar e adquiro lock
                System.out.println("Lock atual é null!");
                //NECESSARIO GUARDAR NO LOG?
                lockAtual = meuLock;
                lockAtual.obtido.complete(null);

            }

            else{
                if(lockAtual.equals(meuLock)){
                    //posso mandar
                    lockAtual.obtido.complete(null);
                }

                else{
                    //senao adiciono à fila
                    filaLock.add(meuLock);
                }
            }

            meuLock.obtido.thenAccept(v -> {
                //aqui podemos ver se ja n tem resposta sendo A
                Msg paraMandar = new Msg(nova.id);
                //Só depois de enviar o prepared é que eu vou fazer o lock, ou fazemos antes? Antes
                System.out.println("Posso enviar!");
                enviaPrepared(paraMandar, Address.from(paraMandar.id.coordenador));
            });
            /**
             * Vou agora mandar a resposta para o coordenador
             * ----> Não usei a mesma mensagem, porque posteriormente pode nos dar mais jeito para trabalhar com os varios coordenadores
             * Mais uma vez tou a utilizar o end[0] que posteriormente poderá ser mudado
             */
            //Msg paraMandar = new Msg(nova.id);
            //ms.sendAsync(end[0], assunto, s.encode(paraMandar));

        }, es);

        ms.registerHandler("abortCoordenador", (a,m)-> {
            System.out.println("Recebi abort!");
            Msg nova = s.decode(m);

            System.out.println("SIZE da fila: " + filaLock.size());
            filaLock.removeIf(l -> (l.xid.equals(nova.id)));
            System.out.println("SIZE da fila depois: " + filaLock.size());

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

            System.out.println("Vou enviar ok no abort!");
            //LIBERTAR LOCK
            if((lockAtual != null) && (lockAtual.xid.equals(nova.id))) {
                System.out.println("Sou eu que tenho o lock!");
                libertaLock();
            }
            enviaOk(paraMandar,Address.from(paraMandar.id.coordenador));


        }, es);

        ms.registerHandler("commitCoordenador", (a,m)-> {
            System.out.println("Recebi commit!");
            MsgCommit nova = s.decode(m);

            if(transacoes.containsKey(nova.id) == false){
                System.out.println("Não foi possivel efetuar commit da transacao ... " + nova.id);
            }
            else{
                /**
                 * Primeiro verifico se já não guardei a
                 */
                if(transacoes.get(nova.id).resultado.equals("C")){
                    //se ja tem resposta, responde e n faz nada
                    System.out.println("Já tinha os valores referentes à transacao " + nova.id);
                }else {
                    /**
                     * Tarefas:
                     * --> Guardar o resultado no log
                     * --> Guardar os valores
                     * --> Adicionar a transacao no hashmap
                     */
                    System.out.println("Vou guardar os valores para a transacao " + nova.id);
                    /*for(Map.Entry<Long, byte[]> entry: nova.valores.entrySet()){
                        /**
                         * Senão tiver a key, entao devo de adicionar tudo
                         * Se ja tiver tenho de adicionar os novos valores de bytes ao array e ainda atualizar a transaction
                         */
                    /*    Long key = entry.getKey();
                        byte[] value = entry.getValue();
                        valores.put(key, value);
                    }
                    */
                    this.valores = this.atualizaValores.apply(this.valores, nova.valores);
                    writerLog.append(new LogEntry(nova.id, "C", this.valores));

                    Transaction t = transacoes.get(nova.id);
                    t.resultado = "C";
                    transacoes.put(nova.id, t);
                    libertaLock();
                }
            }

            Msg paraMandar = new Msg(nova.id);
            System.out.println("Vou enviar ok no commit");
            //LIBERTAR LOCK

            enviaOk(paraMandar,Address.from(paraMandar.id.coordenador));


        }, es);
    }
}

