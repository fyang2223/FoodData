FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

RUN apt update && apt install openssh-server sudo -y

# Create a user "sshuser" and group "sshgroup"
#RUN groupadd -f root && useradd -ms /bin/bash -N -g root root
####RUN groupadd -f sshgroup && useradd -ms /bin/bash -g sshgroup sshuser
#RUN groupadd -f sshgroup && id -u sshuser &>/dev/null || useradd -ms /bin/bash -g sshgroup sshuser

# Create sshuser directory in home
####RUN mkdir -p /home/sshuser/.ssh
RUN mkdir -p /root/.ssh

# Copy the ssh public key in the authorized_keys file. The idkey.pub below is a public key file you get from ssh-keygen. They are under ~/.ssh directory by default.
# Not very secure. Next time try mounting the keys vol FIRST then executing this through CMD? ENTRYPOINT? 
COPY ./my_entrypoint.sh /my_entrypoint.sh
COPY ./load_data.sh /load_data.sh

# Expose docker port 22
EXPOSE 22

#CMD ["/usr/sbin/sshd","-D"]