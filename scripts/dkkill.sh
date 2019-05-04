docker ps | grep -v 'CONTAINER ID' | awk '{print $1}' | xargs docker kill
