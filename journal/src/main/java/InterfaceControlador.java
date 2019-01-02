import io.atomix.utils.net.Address;

import java.util.HashMap;
public interface InterfaceControlador {
    HashMap<Address,Object> distribuiPorParticipante(Object o); //funcao que da os participantes
    Object juntaValores(HashMap<Address,Object> o); //funcao inversa à anterior
    //private Consumer<Msg> handlerMensagem;

    HashMap<Address,Object> participantesGet(Object o); //funcao que devolve
    // os participantes de um get
}
