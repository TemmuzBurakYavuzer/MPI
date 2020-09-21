DATA="/home/thales/Desktop/ngram/Data"
OUTPUT="/home/thales/Desktop/ngram/Output/"
NGRAM_SIZE=2

if test "$#" -ne 1; then
     echo "./ngram.sh numberof"
else
    mpiexec -n $1 python3 parallel.py $DATA $OUTPUT $NGRAM_SIZE $NGRAM_SIZE
fi
