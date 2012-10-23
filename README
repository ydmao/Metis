Metis MapReduce
===============

Metis is a multi-core MapReduce library.

Getting started
---------------
Metis is tested on 64-bit Linux.  Some example MapReduce applications are
included.  For a list:
    $ ls app/*.c
    foo.c wc.c wr.c ...

To build and run the example foo.c do:
    $ ./configure
    $ make
    $ obj/foo [args]

Memory allocator
----------------

To build Metis with scalable memory allocator such as jemalloc and flow.  Flow
is our re-implementation of Streamflow that may be open-sourced soon.  You can
link with a specific memory allocator using:
  $ ./configure --with-malloc=<jemalloc|flow>

Running Test
------------

The ./test/run_all.py script runs all the tests mentioned in Metis technical
report. To run the test, you need to download the data files at
http://pdos.csail.mit.edu/metis/data2.tar.gz into the top-level directory of
Metis source tree, unpack it, and execute the following command to generate the
inputs for all applications:
  $ make data_gen

Scalability on Linux
--------------------
Metis can take advantage of Linux super pages to reduce the contentions
on page faults. To enable feature, Metis currently relies on the use
of flow allocator, which will allocate memory from OS in huge pages.

Note there was a scalability bottleneck in Linux kernel's hugepage
allocator. We haven't checked yet whether Linux has fixed the scalability
problem. If you are interested, take a look at the patch in the
linux-patches directory about the problem and our 'fix'.

Other configuration
-------------------
As described in Metis technical report, Metis can be configured to use
different data structures to organize the intermediate key/value pairs,
although we beleive the default configuration is generally efficient
across all workloads. See `./configure --help` for details.

