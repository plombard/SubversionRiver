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
 * JavaBean for handling JSON generation from SVNEntries
 * For the moment, does not handle directories, only files.
 * TODO : handle directories, with changed paths.
 */
@SuppressWarnings("unused")
public class SubversionDocument {

    @Expose private final String path;
    @Expose private final String name;
    @Expose private final String author;
    @Expose private final String repository;
    @Expose private final long revision;
    @Expose private final Date date;
    @Expose private final long size;
    @Expose private final String message;
    @Expose private final String content;

    private static final HashFunction hf = Hashing.md5();


    public SubversionDocument(SVNDirEntry entry, SVNRepository repository, String path) {
        this.path = entry.getURL().toDecodedString();
        this.author = entry.getAuthor();
        this.content = SubversionCrawler.getContent(entry, repository, path);
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
