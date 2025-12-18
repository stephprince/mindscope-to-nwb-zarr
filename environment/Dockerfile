# hash:sha256:709e108d02d70c108e51cc577b161bee450d959daab8af80be7ac77c1e1c2285
ARG REGISTRY_HOST
FROM $REGISTRY_HOST/codeocean/mambaforge3:24.5.0-0-python3.12.4-ubuntu22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG GIT_ASKPASS
COPY git-askpass /

RUN pip3 install -U --no-cache-dir \
    aind-data-schema==2.3.0 \
    hdmf==4.1.2 \
    hdmf-zarr==0.12.0 \
    nwbinspector==0.6.5 \
    pynwb==3.1.3

ADD "https://github.com/coder/code-server/releases/download/v4.100.3/code-server-4.100.3-linux-amd64.tar.gz" /.code-server/code-server.tar.gz
	
RUN cd /.code-server \
    && tar -xvf code-server.tar.gz \
    && rm code-server.tar.gz \
    && ln -s /.code-server/code-server-4.100.3-linux-amd64/bin/code-server /usr/bin/code-server \
    && mkdir -p /.vscode/extensions
