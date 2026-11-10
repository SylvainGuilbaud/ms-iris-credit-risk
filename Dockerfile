ARG IMAGE=intersystemsdc/irishealth-community
ARG IMAGE=intersystemsdc/iris-ml-community
FROM $IMAGE

WORKDIR /home/irisowner/dev

ARG TESTS=0
ARG MODULE="objectscript-template"
ARG NAMESPACE="USER"

## Embedded Python environment
ENV IRISUSERNAME="_SYSTEM"
ENV IRISPASSWORD="SYS"
ENV IRISNAMESPACE="USER"
ENV PYTHON_PATH=/usr/irissys/bin/
ENV PATH="/usr/irissys/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/home/irisowner/bin"

COPY .iris_init /home/irisowner/.iris_init

## pyprod: virtual environment created as root (outside irisowner's home), readable by everyone
USER root
RUN python3 -m venv /opt/pyprod-venv && \
    /opt/pyprod-venv/bin/pip install --no-cache-dir intersystems-pyprod && \
    chmod -R a+rX /opt/pyprod-venv
USER irisowner

RUN --mount=type=bind,src=.,dst=. \
    iris start IRIS && \
	iris session IRIS < iris.script && \
    ([ $TESTS -eq 0 ] || iris session iris -U $NAMESPACE "##class(%ZPM.PackageManager).Shell(\"test $MODULE -v -only\",1,1)") && \
    iris stop IRIS quietly
