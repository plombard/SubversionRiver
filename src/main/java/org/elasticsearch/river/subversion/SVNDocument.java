package org.elasticsearch.river.subversion;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.tmatesoft.svn.core.SVNDirEntry;
import org.tmatesoft.svn.core.io.SVNRepository;

import java.util.Date;

/**
 * POJO for handling JSON generation from SVNEntries
 */
@SuppressWarnings("unused")
public class SVNDocument {

    @Expose private String path;
    @Expose private String name;
    @Expose private String author;
    @Expose private String repository;
    @Expose private long revision;
    @Expose private Date date;
    @Expose private long size;
    @Expose private String message;
    @Expose private String content;

    private static HashFunction hf = Hashing.md5();

    public SVNDocument(SVNDirEntry entry, SVNRepository repository, String path) {
        this.path = entry.getURL().toDecodedString();
        this.author = entry.getAuthor();
        this.content = Browser.getContent(entry, repository, path);
        this.date = entry.getDate();
        this.message = entry.getCommitMessage();
        this.name = entry.getName();
        this.revision = entry.getRevision();
        this.size = entry.getSize();
        this.repository = entry.getRepositoryRoot().getPath();
    }

    public String json() {
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        return gson.toJson(this);
    }

    @Override
    public String toString() {
        return json();
    }

    /**
     * Path and revision should be sufficient to uniquely identify a file@revision
     * @return  a loosely constructed hashcode converted to String
     */
    public String id() {
        return hf.newHasher()
                .putString(path)
                .putLong(revision)
                .hash()
                .toString();
    }

}
