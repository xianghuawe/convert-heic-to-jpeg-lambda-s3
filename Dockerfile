FROM --platform=linux/amd64 python:3.12

WORKDIR /app
ENTRYPOINT [ "tail" , "-f" , "/dev/null" ]
