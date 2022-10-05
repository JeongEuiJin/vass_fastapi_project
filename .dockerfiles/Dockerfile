# 도커 명령어
# docker build -t gunicorn_fastapi . -f .dockerfiles/Dockerfile
# docker run --rm -it -p 9000:80 gunicorn_fastapi

# cache 때문에 수정한것이 안되면 아래걸로 실행
# docker build --no-cache -t gunicorn_fastapi . -f .dockerfiles/Dockerfile

FROM        --platform=linux/amd64 ubuntu:18.04
MAINTAINER  comeinlab@gmail.com

# apt-get packages
RUN         apt-get -y update
RUN         apt-get install -y python-pip
RUN         apt-get install -y git vim

# pyenv
ENV         DEBIAN_FRONTEND=noninteractive
RUN         apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils liblzma-dev
RUN         curl -L https://raw.githubusercontent.com/yyuu/pyenv-installer/master/bin/pyenv-installer | bash
ENV         PATH /root/.pyenv/bin:$PATH
RUN         pyenv install 3.8.13

# zsh
RUN         apt-get -y install zsh
RUN         wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh || true
RUN         chsh -s /usr/bin/zsh

# pyenv settings
RUN         echo 'export PATH="/root/.pyenv/bin:$PATH"' >> ~/.zshrc
RUN         echo 'eval "$(pyenv init -)"' >> ~/.zshrc
RUN         echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.zshrc

# pyenv virtualenv

RUN         pyenv virtualenv 3.8.13 deploy_docker

# pyodbc mssql을 사용하기위해 아래 패키지 설치
RUN         apt-get -y install software-properties-common
RUN         curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN         add-apt-repository "$(wget -qO- https://packages.microsoft.com/config/ubuntu/18.04/mssql-server-2019.list)"
RUN         curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN         apt-get update
RUN         ACCEPT_EULA=Y apt-get -y install msodbcsql17
RUN         ACCEPT_EULA=Y apt-get -y install mssql-tools
RUN         echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.zshrc
RUN         echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.zshrc

RUN         apt-get update
RUN         apt-get -y install unixodbc-dev

COPY        . /app

RUN         /root/.pyenv/versions/deploy_docker/bin/pip install --no-cache-dir -r /app/.requirements/deploy.txt

ENV         LC_ALL=C.UTF-8
ENV         LANG=C.UTF-8
ENV         PYTHONIOENCODING=utf-8
ENV         TZ=Asia/Seoul

COPY        ./start.sh /start.sh
RUN         chmod +x /start.sh

COPY        ./gunicorn_conf.py /gunicorn_conf.py

COPY        ./start-reload.sh /start-reload.sh
RUN         chmod +x /start-reload.sh

COPY        ./app /app
WORKDIR     /app/

ENV         PYTHONPATH=/app
ENV         API_ENV='prod'

EXPOSE 80
# Run the start script, it will check for an /app/prestart.sh script (e.g. for migrations)
# And then will start Gunicorn with Uvicorn
CMD ["/start.sh"]