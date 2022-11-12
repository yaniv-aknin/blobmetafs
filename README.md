# blobmetafs

A FUSE filesystem which lets you browse and manage a collection of binary blobs based on various metadata they have. For example, if you have a library of movies and an SQLite database with their IMDB metadata, `blobmetafs` *might* let you find `Pulp Fiction.mp4` as either `by-year/1994/Pulp Fiction.mp4` or `by-director/Quentin Tarentino/Pulp Fiction.mp4`.

I think this *might* be useful for quite a few personal and analytic/scientific applications. Think about a collection of movies, photos, music files, DNA sequence results, medical imagery, [game replays](https://github.com/yaniv-aknin/fafdata), etc.

Emphasis on "might" because this may or may not happen: it's just an idea I've started playing with over weekends.
