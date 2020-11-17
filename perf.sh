# !/bin/bash


msgp=(0.95 0.75)
oppa=(0.95 0.75)
fail=(0.95 0.15 0.10)
clients=(5 15 20 25 50)
participants=(10 15 20 25)
request=(100 150 200 500)

for f in "${fail[@]}"
do
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
            #make clean
            echo "target/debug/cs380p-2pc -S ${msg} -s ${op} -f ${f} -c ${client} -p ${participant} -r ${request} -m run -v 0 -l ./tmp "
          	target/debug/cs380p-2pc -S ${msg} -s ${op} -f ${f} -c ${client} -p ${participant} -r ${request} -m run -v 0 -l ./tmp  &
          	sleep 5
          	pid=`ps -ef | grep '[c]s380p-2pc' | awk '{print $2}'`
          	echo "Killing: "$pid
          	kill -INT $pid &> /dev/null
          	sleep 2
          	echo "\n"
          	target/debug/cs380p-2pc -S ${msg} -s ${op} -f ${f} -c ${client} -p ${participant} -r ${request} -m check -v 0 -l ./tmp
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
done
