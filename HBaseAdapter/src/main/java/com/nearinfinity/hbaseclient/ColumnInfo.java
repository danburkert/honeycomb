package com.nearinfinity.hbaseclient;

public class ColumnInfo {
    private long id;
    private String name;
    private ColumnMetadata metadata;
    public ColumnInfo(long id, String name, ColumnMetadata metadata) {
        this.id = id;
        this.name = name;
        this.metadata = metadata;
    }

    public long getId() {
        return this.id;
    }

    public String getName() {
        return this.name;
    }

    public ColumnMetadata getMetadata() {
        return this.metadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnInfo that = (ColumnInfo) o;

        if (id != that.id) return false;
        if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }
}
