package master.storm.tools;

/**
 * 
 * @author fran
 */
public class Word implements Comparable<Word>{
    String topic;
    int ocurrences;
    
    public  Word (String t, int o) {
        this.topic = t;
        this.ocurrences = o;
    }

    @Override
    public int compareTo(Word word) {
        return word.ocurrences - this.ocurrences;
    }

    void increment() {
        ocurrences++;
    }
    
    String getTopic () {
        return this.topic;
    }
    
    @Override
    public String toString() {
        return "Word: "+"|Topic: "+topic+", Ocurrence: "+ocurrences;
    }
    
}
