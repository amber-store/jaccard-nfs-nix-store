// Package nfsd serves the fetched references as a read-only tree over
// NFSv4, with the server of Buildbarn's bb-remote-execution.
//
// The root directory holds one name for every reference under the prefix.
// Looking a name up there, or opening it, fetches the reference (package
// refs). Everything below is read from the objects in the packstore (package
// tree), except the contents of the files of a reference that has been
// materialized, which are read from its plain files (package materialize).
// The file handles are those of package handles.
//
// The package builds for Linux alone: Buildbarn's packages do not compile
// elsewhere without the patches its own build applies to their
// dependencies.
package nfsd
