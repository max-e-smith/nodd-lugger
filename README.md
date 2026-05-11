# cruise-lug

CLI tool for downloading mgg geophysics datasets from the NOAA Open Data Dissemination (NODD) hosted cloud using domain-driven criterion.

## installation

### Install module with Go

#### Requirements
Install Go: https://go.dev/doc/install

#### Instructions
TODO

### Install for your platform
FUTURE

## usage
```clug <command> [options] [subcommand] [subcommand options] <arguments> <target>```

### Global Options

-b --background (default: false)
runs the download process in the background.

-c --space-check (default: false)
will attempt checking target's disk space before downloading.

-v --verbose (default: false)
includes additional output in the console.

-d --dry-run (default: false)
will perform a dry run of command, skipping file download.

-p --parallel <number> (default: 3)
determines the number of parallel downloads for a request.

### Commands
- mb
- csb
- wcd
- path
- help

#### MB

##### options
- help

##### subcommands
 - survey (PENDING)
 - order (FUTURE)

##### Examples

---
~~~
Downloading multibeam data as specified in a manifest, one useing the url option 
to specify a manifest served as the provided url and another using the file option 
to specify a local file-based manifest, to a target directory.
~~~
```clug mb order -u https://manifest/url /target/download/directory```

```clug mb order -f /path/to/file/manifest /target/download/directory```

---
~~~
Download multibeam data from the noaa open dissemnation bucket that has been resolved
using the survey name while also increasing the default number of parallel workers 
to 5 to the target local disk location specified:
~~~
```clug mb survey -bv -p 5 --source=nodd nf2307 fk2114 /target/download/directory```

---

#### WCD

##### options
- help

##### subcommands
- survey (PENDING)
- order (PENDING)

##### Examples

```clug wcd order -u https://manifest/url /target/download/directory```

```clug wcd order -f /path/to/file/manifest /target/download/directory```

#### CSB 

##### options
- help

##### subcommands
- survey (PENDING)

#### PATH

##### options
- help
- 

##### Examples

~~~
Download data resolved recursively, starting with the cloud path prefix(es)
specified, while using the dry-run flag to skip file download and the verbose option 
set in order to increase logging output:
~~~
```clug path -dv s3://bucketname:prefix1 ... /target/download/directory```

---
