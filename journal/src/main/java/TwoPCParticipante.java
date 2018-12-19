import io.atomix.cluster.messaging.ManagedMessagingService;
import io.atomix.cluster.messaging.impl.NettyMessagingService;
import io.atomix.storage.journal.SegmentedJournal;
import io.atomix.storage.journal.SegmentedJournalReader;
import io.atomix.storage.journal.SegmentedJournalWriter;
import io.atomix.utils.net.Address;
import io.atomix.utils.serializer.Serializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class TwoPCParticipante extends TwoPC{

    private HashMap<Integer, Transaction> transacoes = new HashMap<>();

    /**
     * @valores Para guadar os pares chave-valor
     */
    private HashMap<Long, byte[]> valores = new HashMap<>();


    private CompletableFuture<Void> enviaOk(Msg m, Address coord) {

        return ms.sendAsync(coord, "ok", s.encode(m))
            .thenCompose(aux -> {
                return CompletableFuture.completedFuture(null);
        });
    }

    private CompletableFuture<Void> enviaPrepared(Msg m, Address coord){

        return ms.sendAsync(coord, "prepared", s.encode(m))
            .thenCompose(aux -> {
                return CompletableFuture.completedFuture(null);
        });
    }

    private CompletableFuture<Void> enviaAbort(Msg m, Address coord){

        return ms.sendAsync(coord, "abort", s.encode(m))
            .thenCompose(aux -> {
                return CompletableFuture.completedFuture(null);
        });
    }


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
                    try {
                        enviaPrepared(paraMandar,end[0]).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    break;
                case ("A"):
                    try {
                        enviaAbort(paraMandar,end[0]).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    break;

                case ("C"):
                    try {
                        enviaOk(paraMandar,end[0]).get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
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





    public TwoPCParticipante(Address[] e, int id, ManagedMessagingService ms){

        super(e,id,ms);


        System.out.println("TamEnd: " + end.length);

        recuperaLogParticipante();

        ms.registerHandler("prepared", (a,m)-> {
            System.out.println("Recebi prepared!");
            Msg nova = s.decode(m);

            /**
             * Vou verificar primeiro se já recebi uma resposta para este notificação, se sim tenho de dar a mesma
             */
            if(transacoes.containsKey(nova.id)){
                Msg paraMandar = new Msg(nova.id);

                //Para já vou deixar end[0], mas depois o coordenador poderá ser outro ...
                String resultado = transacoes.get(nova.id).resultado;
                switch (resultado){
                    case "P":
                        try {
                            enviaPrepared(paraMandar,end[0]).get();
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        } catch (ExecutionException e1) {
                            e1.printStackTrace();
                        }
                        break;
                    case "A":
                        try {
                            enviaAbort(paraMandar,end[0]).get();
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        } catch (ExecutionException e1) {
                            e1.printStackTrace();
                        }
                        break;
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
                Msg paraMandar = new Msg(nova.id);
                try {
                    enviaAbort(paraMandar,end[0]).get();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (ExecutionException e1) {
                    e1.printStackTrace();
                }
            }
            else{
                /**
                 * Vou guardar a minha decisão no Log "P" <- Estou pronto para iniciar
                 * Vou criar uma nova entrada na transacao
                 */
                writerLog.append(new LogEntry(nova.id,"P", null));
                Transaction t = new Transaction(nova.id, "P");
                transacoes.put(nova.id, t);
                Msg paraMandar = new Msg(nova.id);
                try {
                    enviaPrepared(paraMandar,end[0]).get();
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                } catch (ExecutionException e1) {
                    e1.printStackTrace();
                }
            }

            /**
             * Vou agora mandar a resposta para o coordenador
             * ----> Não usei a mesma mensagem, porque posteriormente pode nos dar mais jeito para trabalhar com os varios coordenadores
             * Mais uma vez tou a utilizar o end[0] que posteriormente poderá ser mudado
             */
            //Msg paraMandar = new Msg(nova.id);
            //ms.sendAsync(end[0], assunto, s.encode(paraMandar));

        }, es);

        ms.registerHandler("abort", (a,m)-> {
            System.out.println("Recebi abort!");
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
            try {
                System.out.println("Vou enviar ok no abort!");
                enviaOk(paraMandar,end[0]).get();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            } catch (ExecutionException e1) {
                e1.printStackTrace();
            }

        }, es);

        ms.registerHandler("commit", (a,m)-> {
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
                    System.out.println("Já tinha os valores referentes à transacao " + nova.id);
                }else {
                    /**
                     * Tarefas:
                     * --> Guardar o resultado no log
                     * --> Guardar os valores
                     * --> Adicionar a transacao no hashmap
                     */
                    System.out.println("Vou guardar os valores para a transacao " + nova.id);
                    for(Map.Entry<Long, byte[]> entry: nova.valores.entrySet()){
                        /**
                         * Senão tiver a key, entao devo de adicionar tudo
                         * Se ja tiver tenho de adicionar os novos valores de bytes ao array e ainda atualizar a transaction
                         */
                        Long key = entry.getKey();
                        byte[] value = entry.getValue();
                        valores.put(key, value);
                    }
                    writerLog.append(new LogEntry(nova.id, "C", valores));

                    Transaction t = transacoes.get(nova.id);
                    t.resultado = "C";
                    transacoes.put(nova.id, t);
                }
            }

            Msg paraMandar = new Msg(nova.id);
            try {
                System.out.println("Vou enviar ok no commit");
                enviaOk(paraMandar,end[0]).get();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            } catch (ExecutionException e1) {
                e1.printStackTrace();
            }

        }, es);
    }
}

