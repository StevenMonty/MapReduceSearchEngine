import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;
import java.util.TreeSet;

public class IndexTerm implements Comparable<IndexTerm>, Serializable {

    private static final long serialVersionUID = 1L;

    private String term;                            // The word being indexed
    private int frequency;                          // Total term occurrences across all docs
    private TreeSet<IndexDocument> occurrences;     // All documents this term appeared in

    public IndexTerm(String term) {
        this.term = term;
        this.frequency = 0;
        this.occurrences = new TreeSet<>();
    }

    public IndexTerm(String term, int frequency, TreeSet<IndexDocument> occurrences) {
        this.term = term;
        this.frequency = frequency;
        this.occurrences = occurrences;
    }

    public IndexTerm(String term, int frequency, ArrayList<IndexDocument> list) {
        this.term = term;
        this.frequency = frequency;
        this.occurrences = new TreeSet<>(list);
    }

    public void addDocument(IndexDocument doc) {
        this.occurrences.add(doc);
        this.frequency += doc.getFrequency();
    }

    public void addDocuments(ArrayList<IndexDocument> docs) {
        this.occurrences.addAll(docs);
        docs.forEach(d -> this.frequency += d.getFrequency());
    }

    public String getTerm() {
        return this.term;
    }

    public int getFrequency() {
        return this.frequency;
    }

    public TreeSet<IndexDocument> getOccurrences() {
        return this.occurrences;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexTerm indexTerm = (IndexTerm) o;
        return term.equals(indexTerm.term);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term);
    }

    @Override
    public String toString() {
        return "Term{" +
                "term:'" + term + '\'' +
                ", freq:" + frequency +
                ", occurrences:" + occurrences +
                '}';
    }

    @Override
    public int compareTo(IndexTerm o) {
        int cmp = Integer.compare(this.frequency, o.frequency);
        if (cmp == 0)
            cmp = this.term.compareTo(o.term);

        return (-1 * cmp); // Sort Descending
    }

}
