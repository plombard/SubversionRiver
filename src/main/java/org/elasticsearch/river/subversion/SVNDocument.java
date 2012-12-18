package org.elasticsearch.river.subversion;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.tmatesoft.svn.core.SVNDirEntry;
import org.tmatesoft.svn.core.io.SVNRepository;

import java.io.IOException;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    /**
     *  Isn't there a nicer way to implements this ?
     *  I already have a nice json perfectly formatted just above,
     *  why should I have to manually re-input all the mapping ? :'(
     * @return an XContentBuilder ready to use for indexation
     * @throws java.io.IOException
     */
    public XContentBuilder json() throws IOException {
        XContentBuilder out = jsonBuilder()
                .startObject()
                .field("path", path)
                .field("author", author)
                .field("content", content)
                .field("date", date)
                .field("message", message)
                .field("name", name)
                .field("revision", revision)
                .field("size", size)
                .field("repository", repository);

        return out.endObject();
    }

    @Override
    public String toString() {
        return toJson();
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
