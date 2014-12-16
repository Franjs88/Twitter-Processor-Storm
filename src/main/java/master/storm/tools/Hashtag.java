package master.storm.tools;

import java.io.Serializable;

/**
 * Esta clase corresponde a la entidad hashtag que debe crearse por cada tweet
 * que contenga la etiqueta '#' seguida de una palabra.
 * @author fran
 */
public class Hashtag implements Serializable {
    String usuario;
    String paisOrigen;
    String palabra;
    long timestamp;
    
    public Hashtag (String user, String country, String word, long timestamp_ms) {
        this.usuario = user;
        this.paisOrigen = country;
        this.palabra = word;
        this.timestamp = timestamp_ms;
    }    

    public String getUsuario() {
        return usuario;
    }

    public void setUsuario(String usuario) {
        this.usuario = usuario;
    }

    public String getPaisOrigen() {
        return paisOrigen;
    }

    public void setPaisOrigen(String paisOrigen) {
        this.paisOrigen = paisOrigen;
    }

    public String getPalabra() {
        return palabra;
    }

    public void setPalabra(String palabra) {
        this.palabra = palabra;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    
}