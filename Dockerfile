FROM r-base:4.2.3
ENV DEBIAN_FRONTEND=noninteractive
SHELL ["/bin/bash", "-c"]
RUN wget "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-$(uname)-$(uname -m).sh" &&  \
    bash Miniforge3-$(uname)-$(uname -m).sh -b -p /miniforge && rm Miniforge3-$(uname)-$(uname -m).sh
RUN /miniforge/bin/mamba init && source /root/.bashrc

ADD environment.yml .
RUN /miniforge/bin/mamba env create --name synth_firm -f environment.yml
RUN source /miniforge/bin/activate synth_firm
ENV PATH /miniforge/envs/synth_firm/bin:$PATH

ADD requirements.R .
RUN Rscript requirements.R

WORKDIR /app

COPY configs configs
COPY input_generation input_generation
COPY scenario_analysis scenario_analysis
COPY SynthFirm_parameters SynthFirm_parameters
COPY utils utils
COPY SynthFirm.conf SynthFirm.conf
COPY SynthFirm_run.py SynthFirm_run.py