package org.elasticsearch.river.subversion;

import com.google.gson.Gson;
import org.tmatesoft.svn.core.SVNDirEntry;
import org.tmatesoft.svn.core.io.SVNRepository;

import java.util.Date;

/**
 * POJO for handling JSON generation from SVNEntries
 */
public class SVNDocument {

    private String path;
    private String name;
    private String author;
    private String repository;
    private long revision;
    private Date date;
    private long size;
    private String message;
    private String content;


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

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public String toString() {
        return toJson();
    }

}
