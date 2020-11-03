# !/bin/bash


msgp=(1.0)
oppa=(1.0)
clients=(1 2 4 8 16)
participants=(1 2 4 8 16)
request=(1 2 4 8 16)




for msg in "${msgp[@]}"
do
  for op in "${oppa[@]}"
  do
    for client in "${clients[@]}"
    do
      for participant in "${participants[@]}"
      do
        for request in "${request[@]}"
        do
            echo "********** RUN **************"
            echo "target/debug/cs380p-2pc -S ${msg} -s ${op} -c ${client} -p ${participant} -r ${request} -m run -v 0"
          	target/debug/cs380p-2pc -S ${msg} -s ${op} -c ${client} -p ${participant} -r ${request} -m run -v 0 &
          	sleep 2
          	pid=`ps | grep target/debug/cs380p-2pc | cut -d' ' -f1`
          	#echo $pid
          	kill -INT $pid
          	sleep 1
          	echo "********** CHECK **************"
          	target/debug/cs380p-2pc -S ${msg} -s ${op} -c ${client} -p ${participant} -r ${request} -m check -v 0
        done
      done
    done
  done
done
