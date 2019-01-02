import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

public class ControlaControlador implements InterfaceControlador {

    public ArrayList<Address> parts = new ArrayList<>();

    public ControlaControlador(Address[] end){
        for(Address a: end){
            parts.add(a);
        }
    }


    @Override
    public HashMap<Address, Object> distribuiPorParticipante(Object o) {
        HashMap<Address,Object> participantes = new HashMap<>();
        HashMap<Long,byte[]> valores = (HashMap<Long,byte[]>)o;
        for(Long aux : valores.keySet()){
            int resto = (int)(aux % (parts.size())); //para já o coordenador n participa
            Object obj = participantes.get(parts.get(resto));
            HashMap<Long,byte[]> auxiliar = (HashMap<Long,byte[]>) obj;
            if(auxiliar == null){
                auxiliar = new HashMap<>();
            }
            auxiliar.put(aux,valores.get(aux));
            participantes.put(parts.get(resto),auxiliar);
        }
        return participantes;
    }

    @Override
    public Object juntaValores(HashMap<Address, Object> part) {
        HashMap<Long,byte[]> res = new HashMap<>();
        for(Object o: part.values()){
            HashMap<Long,byte[]> a = (HashMap<Long,byte[]>)o;
            res.putAll(a);
        }

        return res;
    }

    @Override
    public HashMap<Address, Object> participantesGet(Object chavesInput) {
        HashMap<Address,Object> participantes = new HashMap<>();
        Collection<Long> keys = (Collection<Long>)chavesInput;
        for(Long aux : keys){
            int resto = (int)(aux % (parts.size())); //para já o coordenador n participa
            Object obj = participantes.get(parts.get(resto));
            Collection<Long> auxiliar = (Collection<Long>) obj;
            if(auxiliar == null){
                auxiliar = new HashSet<>();
            }
            auxiliar.add(aux);
            participantes.put(parts.get(resto),auxiliar);
        }
        return participantes;
    }
}
