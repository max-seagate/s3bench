#!/bin/bash

usage() {
   echo "Usage: $0 -K <access_key> -S <access_secret> -b <bucketname> -w <workload_type> -i <no. of iterations> -s <no. of samples> -c <no. of clients>"
   exit 1
}

timeStamp() {
    while IFS= read -r line; do
        printf '%s %s\n' "$(date)" "$line";
    done
}

write_read_verify() {
    echo -e "\n-----WRITE-READ-VERIFY INITIATED-----"
    > /var/log/temp_log.log
    for (( i=1; i<=$ITER; i++ ))
    do
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb  -verbose -skipRead -skipCleanup | timeStamp >>/var/log/temp_log.log
        if [ $i -eq 1 ]
        then
            echo -e "\n$BUCKET created !\n"
        fi
        echo "Write" "($i)" "Done"
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb  -verbose -skipWrite -skipCleanup | timeStamp >>/var/log/temp_log.log
        echo "Read" "($i)" "Done"
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb  -verbose -skipRead -skipWrite -validate | timeStamp >>/var/log/temp_log.log
        echo "Validate" "($i)" "Done"
        echo -e "\n"
    done
    echo -e "Error Report\n"
    grep -Ei "[^:]Errors Count: .*| Operation: .*| Total Requests Count: .*" /var/log/temp_log.log
    echo -e "\nCleared $BUCKET !"
    aws s3 rb s3://$BUCKET --force
    rm -rf /tmp/$BUCKET
    cat /var/log/temp_log.log > /var/log/last_run_s3bench_log.log
    rm -f /var/log/temp_log.log
    cat /var/log/last_run_s3bench_log.log >> /var/log/s3bench_log.log
    echo -e "\n----WRITE-READ-VERIFY DONE----\n"
}

overwrite_verify() {
    echo -e "\n-----OVERWRITE-VERIFY INITIATED-----"
    > /var/log/temp_log.log
    for (( i=1; i<=$ITER; i++ ))
    do
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -skipCleanup -verbose -skipRead -validate | timeStamp >>/var/log/temp_log.log
        if [ $i -eq 1 ]
        then
            echo -e "\n$BUCKET created !\n"
        fi
        echo "Write" "($i)" "Done"
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -skipRead -verbose -validate | timeStamp >>/var/log/temp_log.log
        echo "OverWrite" "($i)" "Done"
        echo "Validate" "($i)" "Done"
        echo -e "\n"
    done
    echo -e "Error Report\n"
    grep -Ei "[^:]Errors Count: .*| Operation: .*| Total Requests Count: .*" /var/log/temp_log.log
    echo -e "\nCleared $BUCKET !"
    aws s3 rb s3://$BUCKET --force
    rm -rf /tmp/$BUCKET
    cat /var/log/temp_log.log > /var/log/last_run_s3bench_log.log
    rm -f /var/log/temp_log.log
    cat /var/log/last_run_s3bench_log.log >> /var/log/s3bench_log.log
    echo -e "\n----OVERWRITE-VERIFY DONE----\n"
}

parallel_write_read_verify() {
    echo -e "\n-----PARALLEL-WRITE-READ-VERIFY INITIATED-----"
    > /var/log/temp_log.log
    for (( i=1; i<=$ITER; i++ ))
    do
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -skipCleanup -verbose -skipRead | timeStamp >>/var/log/temp_log.log &
        if [ $i -eq 1 ]
        then
            echo -e "\n$BUCKET created !\n"
        fi
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -skipCleanup -verbose -skipRead | timeStamp >>/var/log/temp_log.log
        echo "Parallel write" "($i)" "Done"
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -verbose -skipWrite -skipCleanup | timeStamp >>/var/log/temp_log.log
        echo "Read" "($i)" "Done"
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -verbose -skipWrite -skipRead -validate | timeStamp >>/var/log/temp_log.log
        echo "Validate" "($i)" "Done"
        echo -e "\n"
    done
    echo -e "Error Report\n"
    grep -Ei "[^:]Errors Count: .*| Operation: .*| Total Requests Count: .*" /var/log/temp_log.log
    echo -e"\nCleared $BUCKET !"
    aws s3 rb s3://$BUCKET --force
    rm -rf /tmp/$BUCKET
    cat /var/log/temp_log.log > /var/log/last_run_s3bench_log.log
    rm -f /var/log/temp_log.log
    cat /var/log/last_run_s3bench_log.log >> /var/log/s3bench_log.log
    echo -e "\n----PARALLEL-WRITE-READ-VERIFY DONE----\n"
}

write_delete_write_verify() {
    echo -e "\n-----WRITE-DELETE-WRITE-VERIFY INITIATED-----"
    for (( i=1; i<=$ITER; i++ ))
    do
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -skipCleanup -verbose -skipRead | timeStamp >>/var/log/temp_log.log
        if [ $i -eq 1 ]
        then
            echo -e "\n$BUCKET created !\n"
        fi
        echo "Write" "($i)" "Done"
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -verbose -skipWrite -skipRead | timeStamp >>/var/log/temp_log.log
        echo "Delete" "($i)" "Done"
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -verbose -skipCleanup -skipRead | timeStamp >>/var/log/temp_log.log
        echo "Second Write" "($i)" "Done"
        ./s3bench -accessKey $ACCESSKEY -accessSecret $SECRETKEY -bucket $BUCKET -endpoint http://s3.seagate.com -numClients $NCLIENTS -numSamples $NSAMPLES -objectSize 1Mb -verbose -skipWrite -skipRead -validate | timeStamp >>/var/log/temp_log.log
        echo "Validate" "($i)" "Done"
        echo -e "\n"
    done
    echo -e "Error Report\n"
    grep -Ei "[^:]Errors Count: .*| Operation: .*| Total Requests Count: .*" /var/log/temp_log.log
    echo -e "\nCleared $BUCKET !"
    aws s3 rb s3://$BUCKET --force
    rm -rf /tmp/$BUCKET
    cat /var/log/temp_log.log > /var/log/last_run_s3bench_log.log
    rm -f /var/log/temp_log.log
    cat /var/log/last_run_s3bench_log.log >> /var/log/s3bench_log.log
    echo -e "\n----WRITE-DELETE-WRITE-VERIFY DONE----\n"
}

while getopts "w:i:s:c:b:K:S:" opt
do
   case "$opt" in
      w ) WORKLOAD="$OPTARG" ;;
      i ) ITER="$OPTARG" ;;
      s ) NSAMPLES="$OPTARG" ;;
      c ) NCLIENTS="$OPTARG" ;;
      b ) BUCKET="$OPTARG" ;;
      K ) ACCESSKEY="$OPTARG" ;;
      S ) SECRETKEY="$OPTARG" ;;
      ? ) usage ;;
   esac
done

case "$WORKLOAD" in 
    "write_read_verify" ) write_read_verify ;;
    "overwrite_verify" ) overwrite_verify ;;
    "parallel_write_read_verify" ) parallel_write_read_verify ;;
    "write_delete_write_verify" ) write_delete_write_verify ;;
esac


echo -e "Log file for this run : /var/log/last_run_s3bench_log.log !\n"
echo -e "Log file : /var/log/s3bench_log.log !\n"