FROM apache/hadoop:3.4.1
# Set workdir
WORKDIR /opt/hadoop/
# Install package dependencies (make)
RUN bash -c "\
if ! rpm -q make > /dev/null 2>&1; then \
    echo 'Make not found. Installing...'; \
    sudo sed -i -e '/^mirrorlist/d;/^#baseurl=/{s,^#,,;s,/mirror,/vault,;}' /etc/yum.repos.d/CentOS*.repo && \
    sudo yum -y update && \
    sudo yum clean all && \
    sudo yum install -y make; \
else \
    echo 'Make is already installed.'; \
fi"
# Copy Makefile from host
COPY Makefile ./
# Define metadata storage directory
ENV ENSURE_NAMENODE_DIR="/tmp/hadoop-root/dfs/name"
# Install Spark, Python3, Zeppelin
RUN bash -c "\
sudo make install-spark; \
sudo make install-python3; \
make install-zeppelin;"
# Start zeppelin and namenode by default
CMD bash -c "make start-zeppelin && make start-namenode"