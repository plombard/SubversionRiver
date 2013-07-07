# Subversion River for ElasticSearch #

Subversion River plugin for [ElasticSearch](http://www.elasticsearch.org/)

## Getting Started ##

### Installation ###

After downloading the sources, run the task `mvn package`.

Use the elasticsearch plugin manager to install the plugin :

    $ /path-to/elasticsearch/bin/plugin -url file:./target/release/elasticsearch-river-subversion-0.2.0.zip -install river-subversion

### Creating a Subversion river ###
Just create a new river of type "svn" and give it at least a repository (only local file at the moment) and a path to index ("/" for the entire repos) :

    curl -XPUT 'localhost:9200/_river/mysvnriver/_meta' -d '{
       "type": "svn",
       "svn": {
        "repos": "file:///myRepository",
        "path":  "mySvnModule/trunk"
       }
    }'

### Data indexed ###
For the moment the river indexes only a few metadatas from svn, mainly the file content (I wrote it for full-text searches) :
**path**, **name**, **author**, **repository**, **revision**, **date**, **size**, **message** and **content**.
Please be aware that the mapping did change from the previous version, as the indexing of revisions is now preferred to the indexing of single documents.


## Credits ##

These sources borrows *heavily* from :

- SVNKit examples by the folks @TMate : [http://wiki.svnkit.com/Managing_Repository_With_SVNKit](http://wiki.svnkit.com/Managing_Repository_With_SVNKit)

- ElasticSearch plugins how-to by @jfarrell : [http://jfarrell.github.com/](http://jfarrell.github.com/)

- ElasticSearch RSS River by @dadoonet : [https://github.com/dadoonet/rssriver](https://github.com/dadoonet/rssriver)

- ElasticSearch Confluence River by @obazoud : [https://github.com/obazoud/elasticsearch-river-confluence](https://github.com/obazoud/elasticsearch-river-confluence)

- ElasticSearch Tests Helpers by @tlrx : [https://github.com/tlrx/elasticsearch-test](https://github.com/tlrx/elasticsearch-test)

## Copyright ##

I put most of the code under Apache 2.0 licence.
