go-benchmark
============

This includes instructions for how to install Go locally and get benchmarks compiling.

The documentation is in http://golang.org/doc/install. Here is what I did:
 - Download the archive from https://code.google.com/p/go/wiki/Downloads?tm=2 and installed unpacked it to $HOME/go
 - Put the following in my .bashrc file:
  - export GOROOT=$HOME/go
  - export PATH=$PATH:$GOROOT/bin
 - created my work directory, $HOME/gowork
 - Put the following in my .bashrc file:
  - export GOPATH=$HOME/gowork
  - export PATH=$PATH:$GOPATH/bin 
 - created $HOME/gowork/src
 - from GOPATH, run go get github.com/Tokutek/go-benchmark. This should get almost everything needed. When I ran this, the command in step 6 did not automatically download the mgo driver. So, in addition, I ran “go get labix.org/v2/mgo”. I learned this when trying to execute the step below, and seeing compilation fail. Should any other package not be installed, install it with “go get …”
 - From there, within any directory in go-benchmark (e.g. sysbench), run go build. That compiles the sysbench binary used for a benchmark. To install it to $GOPATH/bin, which is in your path, run go install. Add the “-o” option to name the executable. So, for example, in $GOPATH/src/github.com/Tokutek/go-benchmark/benchmarks/iibench/bin, run “go build -o iibench” and that will produce the executable file “iibench”. Without “-o”, it will produce an executable named “bin”.

At this point, you should be able to compile any benchmark/test we have.

To learn how to run any benchmark executable, start it with --help.

To run iibench with defaults of 1 writer, no readers, and for the duration of 1 hour, run:
./iibench -host=YOUR_SERVER:YOUR_PORT -create=true


[![GoDoc](https://godoc.org/github.com/Tokutek/go-benchmark?status.png)](https://godoc.org/github.com/Tokutek/go-benchmark)
