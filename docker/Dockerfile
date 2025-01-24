# ROS distribution to use
ARG ROS_DISTRO=humble

FROM osrf/ros:${ROS_DISTRO}-desktop as base
ENV ROS_DISTRO=${ROS_DISTRO}
SHELL ["/bin/bash", "-c"]

# Install basic apt packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    git libcanberra-gtk-module libcanberra-gtk3-module fuse3 libfuse2 libqt5svg5-dev \
    python3-pip python3-opencv python3-tk python3-pyqt5.qtwebengine mesa-utils \
    tmux ros-dev-tools gdb gdbserver vim zsh clang curl libssl-dev

COPY docker/setup.bash /opt/setup.bash
COPY docker/entrypoint.sh /opt/entrypoint.sh
RUN chmod a+rx /opt/entrypoint.sh\
    && chmod a+rx /opt/setup.bash

ENTRYPOINT ["/opt/entrypoint.sh"]

FROM base as dev

# Dev container arguments
ARG USERNAME=devuser
ARG UID=1000
ARG GID=${UID}

RUN mkdir -p /ws/src
WORKDIR /ws

# Create new user and home directory
RUN groupadd --gid $GID $USERNAME \
    && useradd --uid ${GID} --gid ${UID} --create-home ${USERNAME} \
    && echo ${USERNAME} ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/${USERNAME} \
    && chmod 0440 /etc/sudoers.d/${USERNAME} \
    && mkdir -p /home/${USERNAME} \
    && chown -R ${UID}:${GID} /home/${USERNAME}

# Set the ownership of the overlay workspace to the new user
RUN chown -R ${UID}:${GID} /ws

RUN groupadd fuse\
    && usermod -aG fuse,video ${USERNAME}

# Set the user and source entrypoint in the user's .bashrc file
RUN echo Defaults secure_path = /opt/ros/humble/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin > /etc/sudoers.d/secure_path \
    && chmod 0440 /etc/sudoers.d/secure_path

RUN echo "source /opt/entrypoint.sh" >> /root/.profile
USER ${USERNAME}
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
RUN echo "source /opt/entrypoint.sh" >> /home/${USERNAME}/.bashrc
