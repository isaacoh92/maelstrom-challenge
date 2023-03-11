# Maelstrom-challenge

These challenges have been tested against [Maelstrom 0.2.3](https://github.com/jepsen-io/maelstrom/releases/tag/v0.2.3)

First, download the tarball and unpack it. Located inside the directory is the `maelstrom` binary that we will use to run our tests.

For each challenge:
* go into the corresponding directory
* fetch required libraries and dependencies using `go get .`
* compile the program using `go install .`

Now, we can run tests using the `maelstrom` binary
* e.g `./maelstrom test -w echo --bin ~/go/bin/maelstrom-echo --node-count 1 --time-limit 10`)