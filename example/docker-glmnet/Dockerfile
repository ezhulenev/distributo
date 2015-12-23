FROM ubuntu:15.10

MAINTAINER ezhulenev

RUN apt-get update

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Install R                            - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

RUN apt-get install -y r-base
RUN mkdir /install
COPY install/* /install/
RUN R CMD BATCH /install/r-libraries.R /install/r-libraries.out

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Model files                          - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

RUN mkdir /model
COPY model/* /model/

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Interface                            - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

ENTRYPOINT ["Rscript", "/model/run-modeling.R"]
