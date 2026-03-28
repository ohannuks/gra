import pylab as plt
import numpy as np
import gwpy.timeseries
import scienceplots
plt.style.use(['science', 'ieee', 'bright'])
def plot_strain(data):
    detectors = data.keys()
    fig, ax = plt.subplots(len(detectors), 1, figsize=(10, 2*len(detectors)), sharex=True)
    # Make sure ax is always a list, even if there's only one detector
    if len(detectors) == 1:
        ax = [ax]
    for i, det in enumerate(detectors):
        strain_data = data[det] # Time series object (gwpy)
        time = strain_data.times
        strain = strain_data.value
        ax[i].plot(time, strain, label=det)
        ax[i].set_ylabel('Strain')
        ax[i].legend()
    ax[-1].set_xlabel('Time (s)')
    return fig, ax

