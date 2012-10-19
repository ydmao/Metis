#!/bin/bash

pushd data_tool
bash clean.sh
g++ gen.cc -o gen
popd

TOP=./data

# Generate wr input with many keys and many duplicates
dd if=$TOP/100MB_1M_Keys.txt of=$TOP/wr/5MB.txt~ count=1 bs=5000000
i=0
cp $TOP/wr/5MB.txt~ $TOP/wr/800MB.txt
while [ "$i" -lt "160" ]; do
  cat $TOP/wr/5MB.txt~ >> $TOP/wr/800MB.txt
  i=$((i+1))
done
rm $TOP/wr/5MB.txt~

# Generate wr input with many keys and many duplicates, but unpredicatable
./data_tool/gen 500000 4 > $TOP/wr/500MB.txt

# Generate hist input
i=0
while [ "$i" -lt "887" ]; do
  cat $TOP/3MB.bmp >> $TOP/hist-2.6g.bmp
  i=$((i+1))
done

# Generate linear regression input
dd if=/dev/urandom of=$TOP/lr_10MB.txt~ count=1024 bs=10240
i=0
while [ "$i" -lt "400" ]; do
  cat $TOP/lr_10MB.txt~ >> $TOP/lr_4GB.txt
  i=$((i+1))
done
rm $TOP/lr_10MB.txt~

# Generate string match input
i=0
while [ "$i" -lt "100" ]; do
  cat $TOP/wc/10MB.txt >> $TOP/sm_1GB.txt
  i=$((i+1))
done

