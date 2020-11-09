# !/bin/bash


msgp=(1)
oppa=(1)
clients=(5 15 20 25 50)
participants=(15 20)
request=(100 150 200)


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
            echo "\n******************** ITR ************************"
            echo "\n"
            echo "target/debug/cs380p-2pc -S ${msg} -s ${op} -c ${client} -p ${participant} -r ${request} -m run -v 0"
          	target/debug/cs380p-2pc -S ${msg} -s ${op} -c ${client} -p ${participant} -r ${request} -m run -v 0 &
          	sleep 2
          	pid=`ps | grep cs380p-2pc | cut -d' ' -f1`
          	echo $pid
          	kill -INT $pid &> /dev/null
          	sleep 1
          	echo "\n"
          	target/debug/cs380p-2pc -S ${msg} -s ${op} -c ${client} -p ${participant} -r ${request} -m check -v 0
          	RC=$?
          	if [[ $RC -ne 0 ]];then
          	  echo "\n ERROR "
          	  exit 1
          	fi
        done
      done
    done
  done
done
