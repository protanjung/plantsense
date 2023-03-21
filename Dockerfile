FROM ros:noetic-ros-base

RUN sudo apt update \
    && sudo apt install -y \
    wget \
    && sudo rm -rf /var/lib/apt/lists/*

RUN wget https://bootstrap.pypa.io/get-pip.py \
    && python3 get-pip.py

RUN pip install \
    requests \
    mysql-connector-python

RUN mkdir -p /root/plantsense
COPY ./src /root/plantsense/src
RUN /bin/bash -c "source /opt/ros/noetic/setup.bash && cd /root/plantsense && catkin_make"

RUN mkdir -p /root/plantsense-data
VOLUME [ "/root/plantsense-data" ]

COPY ./plantsense_entrypoint.sh /

ENTRYPOINT [ "/plantsense_entrypoint.sh" ]
CMD [ "roslaunch", "ps_routine", "start.launch" ]