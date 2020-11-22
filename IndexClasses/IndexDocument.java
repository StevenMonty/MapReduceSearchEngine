import java.io.Serializable;
import java.util.Objects;

public class IndexDocument implements Comparable<IndexDocument>, Serializable {

    private static final long serialVersionUID = 1L;

    private String docName;     // The name of this documents file
    private String docDir;      // The directories this document was in
    private int frequency;      // The number of time the term this document belongs to occured in this doc
    private int docID;          // Generated document Identifier

    public IndexDocument(String docName, String docDir, int frequency) {
        this.docName = docName;
        this.docDir = docDir;
        this.frequency = frequency;
        this.docID = Math.abs(this.hashCode());
    }

    public IndexDocument(String docName, String docDir, String frequency, String docID) {
        this.docName = docName;
        this.docDir = docDir;
        this.frequency = Integer.parseInt(frequency);
        this.docID = Integer.parseInt(docID);
    }

    public int getFrequency() {
        return this.frequency;
    }

    public int getDocID() {
        return docID;
    }

    public String getDocName() {
        return docName;
    }

    public String getDocDir() {
        return docDir;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexDocument that = (IndexDocument) o;
        return docID == that.docID &&
                docName.equals(that.docName) &&
                docDir.equals(that.docDir);
    }

    @Override
    public int hashCode() {
        return Objects.hash(docDir, docName);
    }

    @Override
    public String toString() {
        return "Doc{" +
                "ID:" + docID +
                ", Dir:'" + docDir + '\'' +
                ", Name:'" + docName + '\'' +
                ", Freq:" + frequency +
                '}';
    }

    @Override
    public int compareTo(IndexDocument o) {

        int cmp = Integer.compare(this.frequency, o.frequency);
        if (cmp == 0)
            cmp = this.docName.compareTo(o.docName);

        return (-1 * cmp); // Sort Descending
    }
}
