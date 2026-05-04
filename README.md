# cruise-lug

CLI tool for downloading mgg geophysics datasets from the NOAA Open Data Dissemenation (NODD) hosted cloud using domain driven criterion.

## installation

### Install module with Go

#### Requirements
Install Go: https://go.dev/doc/install

#### Instructions
TODO

### Install for your platform
FUTURE

## usage
clug [command] [options] [arguments] [target]

### Commands
- order
- mb
- csb
- wcd
- help

### Options

-b --background (default: false)
runs the download process in the background.

-c --space-check (default: false)
will attempting checking target's disk space before downloading.

-v --verbose (default: false)
includes additional output in the console.

-d --dry-run (default: false)
will perform a dry run of command, skipping file download.

-p --parallel <number> (default: 3)
determines the number of parallel downloads for a request.

# Examples

---
~~~
Downloading multibeam data from source and source paths——as specified in
a file manifest——to a target directory while using the background option to
run the download in the background and the space-check option to estimate disk
space usage versus available disk space of target prior to download.
~~~
```clug order -bc /path/to/manifest.json /target/download/directory```

---
~~~
Download multibeam data from the noaa open dissemnation bucket that has been resolved
using the survey name while also increasing the default number of parallel workers 
to 5 to the target local disk location specified:
~~~
```clug mb --nodd --survey -p=5 -bv nf2307 fk2114 /target/download/directory```

---

~~~
Download multibeam data from the noaa open dissemnation bucket resolved recursively 
starting with the cloud path prefix specified while using the dry-run flag to skip
file download and the verbose option set in order to increase logging output:
~~~
```clug mb ---nodd --path -dv mb/ships/falkor/fk200429 /target/download/directory```

---