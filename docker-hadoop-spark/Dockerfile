FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

RUN apt update && apt install openssh-server sudo -y

# Create a user "sshuser" and group "sshgroup"
RUN groupadd -f sshgroup && useradd -ms /bin/bash -g sshgroup sshuser
#RUN groupadd -f sshgroup && id -u sshuser &>/dev/null || useradd -ms /bin/bash -g sshgroup sshuser

# Create sshuser directory in home
RUN mkdir -p /home/sshuser/.ssh

# Copy the ssh public key in the authorized_keys file. The idkey.pub below is a public key file you get from ssh-keygen. They are under ~/.ssh directory by default.
# Not very secure. Next time try mounting the keys vol FIRST then executing this through CMD? ENTRYPOINT? 
#COPY ../KEYS/AirflowKey.pub /home/sshuser/.ssh/authorized_keys
COPY ./my_entrypoint.sh /my_entrypoint.sh

# change ownership of the key file. 
#RUN chown sshuser:sshgroup /home/sshuser/.ssh/authorized_keys && chmod 700 /home/sshuser/.ssh/authorized_keys

# Start SSH service
#RUN service ssh start

# Expose docker port 22
EXPOSE 22
#CMD ["/usr/sbin/sshd","-D"]