FROM b.gcr.io/tensorflow/tensorflow

MAINTAINER Eugene Zhulenev

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Add TensforFlow ImageNet model       - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

ADD http://download.tensorflow.org/models/image/imagenet/inception-2015-12-05.tgz /tensorflow/imagenet
ADD http://image-net.org/imagenet_data/urls/imagenet_fall11_urls.tgz /tensorflow/imagenet

# NOTE: For dev only
# COPY /inception-2015-12-05.tgz /tensorflow/imagenet/
# COPY /imagenet_fall11_urls.tgz /tensorflow/imagenet/

RUN tar -xvzf /tensorflow/imagenet/inception-2015-12-05.tgz -C /tensorflow/imagenet/
RUN tar -xvzf /tensorflow/imagenet/imagenet_fall11_urls.tgz -C /tensorflow/imagenet/

RUN rm -f /tensorflow/imagenet/inception-2015-12-05.tgz
RUN rm -f /tensorflow/imagenet/imagenet_fall11_urls.tgz

COPY /imagenet/classify_image.py /tensorflow/imagenet/

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Amazon CLI                           - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

RUN pip install awscli

# - - - - - - - - - - - - - - - - - - - - - - - - #
# - -    Interface                            - - #
# - - - - - - - - - - - - - - - - - - - - - - - - #

COPY /bin /bin
RUN chmod +x /bin/run-inference.sh

ENTRYPOINT ["/bin/run-inference.sh"]
