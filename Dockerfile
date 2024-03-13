FROM ros:noetic-ros-base

RUN apt update \
    && apt install -y \
    libpq-dev \
    python3-pip \
    openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install \
    requests \
    pandas \
    psycopg2 \
    prometheus_client \
    flask \
    flask_cors \
    pulp \
    pyromat \
    tabula-py

RUN mkdir -p /root/plantsense
COPY ./src /root/plantsense/src
RUN /bin/bash -c "source /opt/ros/noetic/setup.bash && cd /root/plantsense && catkin_make"

RUN mkdir -p /root/plantsense-data
VOLUME [ "/root/plantsense-data" ]

COPY ./plantsense_entrypoint.sh /

ENTRYPOINT [ "/plantsense_entrypoint.sh" ]
CMD [ "roslaunch", "ps_routine", "start.launch" ]