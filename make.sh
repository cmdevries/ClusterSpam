#!/bin/bash
g++-4.8 -I/home/chrisdv/libraries/boost_1_54_0 \
-L/home/chrisdv/libraries/boost_1_54_0/stage/lib \
-std=c++11 -l boost_system -l boost_filesystem -l boost_iostreams -O2 -march=native -mtune=native -o cluster_spam cluster_spam.cpp
