did=$(docker ps | grep $1 | awk '{print $1}' | head -n1)
echo $did
docker exec -it $did /bin/bash
