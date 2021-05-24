#!/bin/bash

for i in {1..10}
do
    ant clean > /dev/null
    ant test > test.txt    
    if [ "$?" -eq 0 ]; then
        echo -n "."
    else
        break
    fi

    ant systemtest > systemtest.txt
    if [ "$?" -eq 0 ]; then
        echo -n "."
    else
        break
    fi

    sleep 1s
done
