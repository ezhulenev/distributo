FROM ubuntu:15.10

MAINTAINER Eugene Zhulenev

RUN apt-get update && \
    apt-get upgrade -y

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Install AWS CLI                      - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

RUN apt-get install -y \
    ssh \
    python \
    python-pip \
    python-virtualenv

RUN pip install awscli

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Install R                            - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

RUN apt-get install -y r-base
COPY install/* /install/
RUN R CMD BATCH /install/r-libraries.R /install/r-libraries.out

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Model files                          - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

COPY model/* /model/

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Interface                            - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

# ENTRYPOINT ["Rscript", "/model/run-modeling.R"]
