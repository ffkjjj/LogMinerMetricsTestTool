import java.util.Objects;

public class LogFile {

    public enum Type {
        ARCHIVE,
        REDO
    }

    private final String fileName;
    private final long firstScn;
    private final long nextScn;
    private final Long sequence;
    private final boolean current;
    private final Type type;

    /**
     * Create a log file that represents an archived log record.
     *
     * @param fileName the file name
     * @param firstScn the first system change number in the log
     * @param nextScn the first system change number in the following log
     * @param sequence the unique log sequence number
     * @param type the log type
     */
    public LogFile(String fileName, long firstScn, long nextScn, Long sequence, Type type) {
        this(fileName, firstScn, nextScn, sequence, type, false);
    }

    /**
     * Creates a log file that represents an online redo log record.
     *
     * @param fileName the file name
     * @param firstScn the first system change number in the log
     * @param nextScn the first system change number in the following log
     * @param sequence the unique log sequence number
     * @param type the type of archive log
     * @param current whether the log file is the current one
     */
    public LogFile(String fileName, long firstScn, long nextScn, Long sequence, Type type, boolean current) {
        this.fileName = fileName;
        this.firstScn = firstScn;
        this.nextScn = nextScn;
        this.sequence = sequence;
        this.current = current;
        this.type = type;
    }

    public String getFileName() {
        return fileName;
    }

    public long getFirstScn() {
        return firstScn;
    }

    public long getNextScn() {
        return isCurrent() ? Long.MAX_VALUE : nextScn;
    }

    public Long getSequence() {
        return sequence;
    }

    /**
     * Returns whether this log file instance is considered the current online redo log record.
     */
    public boolean isCurrent() {
        return current;
    }

    public Type getType() {
        return type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sequence);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof LogFile)) {
            return false;
        }
        final LogFile other = (LogFile) obj;
        return Objects.equals(sequence, other.sequence);
    }
}
