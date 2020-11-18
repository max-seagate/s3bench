#!/usr/bin/env bash

COMBINATION="$1"
FILENAME="test/file.${COMBINATION}"
SPLIT_DIR="test/split.${COMBINATION}"

PARAMS_S3BENCH=$(cat "test/params_s3bench/${COMBINATION}")
PARAMS_SPLIT=$(cat "test/params_split/${COMBINATION}")

rm -f "${FILENAME}"
./s3bench $PARAMS_S3BENCH -testReductionFile "${FILENAME}"

mkdir -p ${SPLIT_DIR}
cd "${SPLIT_DIR}"
split "../../${FILENAME}" $PARAMS_SPLIT

TOTAL=$(ls | wc -l)
UNIQUE=$(sha1sum * | awk '{print $1}' | sort | uniq | wc -l)

compression() {
	for f in $(ls); do
		lz4 $f $f.lz4 2&>1 > /dev/null
	done
	du --bytes --total *.lz4 | tail -n1 | awk '{print $1}'
	rm *.lz4
}

COMPRESSION=$(compression)

UNIQUE_FILES=$(sha1sum * | sort | uniq --check-chars=40 | awk '{print $2}')
mkdir unique
cp $UNIQUE_FILES unique/
cd unique
REDUCTION=$(compression)
cd ..

cd ../..
echo "${UNIQUE} ${TOTAL}" > "test/results_dedup/${COMBINATION}"
echo ${COMPRESSION} > "test/results_compression/${COMBINATION}"
echo ${REDUCTION} > "test/results_reduction/${COMBINATION}"

rm -rf ${SPLIT_DIR}
rm -f "${FILENAME}"
