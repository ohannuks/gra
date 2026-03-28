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
        time = strain_data.times.value
        strain = strain_data.value
        ax[i].plot(time, strain, label=det)
        plt.xlim(time[0], time[-1])
        ax[i].set_ylabel('Strain')
        ax[i].legend()
    ax[-1].set_xlabel('Time (s)')
    return fig, ax
def plot_psd(psds, fig=None):
    detectors = psds.keys()
    if fig == None:
        fig, ax = plt.subplots(len(detectors), 1, figsize=(10, 2*len(detectors)), sharex=True)
    else:
        ax = fig.get_axes()
    # Make sure ax is always a list, even if there's only one detector
    if len(detectors) == 1:
        ax = [ax]
    for i, det in enumerate(detectors):
        f, psd = np.transpose(psds[det])
        ax[i].loglog(f, psd, label=det)
        fmin, fmax = 10, 2048
        ax[i].set_xlim(fmin, fmax)
        ax[i].set_ylim(1e-48, 1e-44)
        ax[i].set_ylabel('PSD')
        ax[i].legend()
    ax[-1].set_xlabel('Frequency (Hz)')
    return fig, ax

