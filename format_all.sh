#!/bin/bash

find ./ -name "*.cpp" -or -name "*.h" -or -name "*.hpp" -or -name "*.cc" -or -name "*.cxx" |grep -v build/ |grep -v third | xargs clang-format -i
