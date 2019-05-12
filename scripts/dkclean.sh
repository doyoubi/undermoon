docker images | grep none | awk '{print $3}' | xargs docker rmi

