FROM r-base:latest

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
# - -    Install R Libraries                  - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

COPY /install /install
RUN R CMD BATCH /install/r-libraries.R /install/r-libraries.out

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Model files                          - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

COPY /model /model

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Interface                            - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

COPY /bin /bin
RUN chmod +x /bin/run-modeling.sh

ENTRYPOINT ["/bin/run-modeling.sh"]
