Here's how to run your code on the word-count MapReduce application. First, make sure the word-count plugin is freshly built:

```
$ go build -race -buildmode=plugin ../mrapps/wc.go
```

In the `main` directory, run the coordinator.

```
$ rm mr-out*
$ go run -race mrcoordinator.go pg-*.txt
```

The `pg-*.txt` arguments to `mrcoordinator.go` are the input files; each file corresponds to one "split", and is the input to one Map task. The `-race` flags runs go with its race detector.

In one or more other windows, run some workers:

```
$ go run -race mrworker.go wc.so
```



```
cat mr-out-* | sort | more
```



```sh
$ bash test-mr.sh
```

