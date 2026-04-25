from gra import data_lvk
from boltons.iterutils import research
import bilby.core.utils.io

# Hardcoded parameters
PRIORITY_WAVEFORMS = ["NRSur7dq4", "SEOBNRv5PHM", "SEOBNRv4PHM", "IMRPhenomXPHM"]
def _choose_priority_waveform(data_dict):
    for waveform in PRIORITY_WAVEFORMS:
        if waveform in data_dict or "C00:"+waveform in data_dict:
            wf = waveform if waveform in data_dict else "C00:"+waveform
            return data_dict[wf]
def _posterior_to_dict(filename):
    with h5py.File(filename, 'r') as h5file:
        data_dict = bilby.core.utils.io.recursively_load_dict_contents_from_group(h5file, "/")
    return _choose_priority_waveform(data_dict)
def _find_dictionary(data_dict, key):
    return research(data_dict, query=lambda p, k, v: k==key)
def _find_dictionary_partial(data_dict, key):
    # Same as above, but partial match is ok
    return research(data_dict, query=lambda p, k, v: key in k)

def read_calibration_envelope_from_posterior(event_name):
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    posterior_dict = _posterior_to_dict(posterior_filename)
    calibration_envelope_path, calibration_envelope = _find_dictionary(posterior_dict, "calibration_envelope")[0]
    calibration_model_path, calibration_model = _find_dictionary(posterior_dict, "calibration_model")[0]
    if calibration_model[0] != 'CubicSpline':
        raise ValueError(f"Expected calibration model to be 'CubicSpline', but got '{calibration_model}'")
    spline_calibration_nodes_path, spline_calibration_nodes = _find_dictionary(posterior_dict, "spline_calibration_nodes")[0]
    return calibration_envelope, calibration_model[0], int(spline_calibration_nodes[0])

def read_prior_from_posterior(event_name):
    from bilby.core.prior import Sine, Cosine, Uniform, Constraint, Gaussian, PowerLaw, LogUniform, JointPrior, PriorDict
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    posterior_dict = _posterior_to_dict(posterior_filename)
    prior_path, prior_dict_unformatted = _find_dictionary(posterior_dict, "analytic")[0]
    prior_dict = {}
    for key in prior_dict_unformatted.keys():
        prior_dict[key] = eval(prior_dict_unformatted[key][0])
    prior = PriorDict(prior_dict)
    return prior

def read_config_from_posterior(event_name):
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    posterior_dict = _posterior_to_dict(posterior_filename)
    config_path, config = _find_dictionary(posterior_dict, "config")[0]
    return config

def read_meta_data_from_posterior(event_name):
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    posterior_dict = _posterior_to_dict(posterior_filename)
    meta_data = posterior_dict['meta_data']['meta_data']
    return meta_data

def read_frequency_cuts_from_posterior(event_name):
    config = read_config_from_posterior(event_name)
    format_str = lambda s: re.sub(r'\{(.*)\}', r'dict(\1)', s).replace(':', '=')
    minimum_frequency_dict = eval(format_str(config['minimum_frequency'][0]))
    maximum_frequency_dict = eval(format_str(config['maximum_frequency'][0]))
    return minimum_frequency_dict, maximum_frequency_dict

def _resample_gwpy_timeseries(ts, sampling_frequency, resampling_method):
    if method == "gwpy":
        return ts.resample(sampling_frequency)
    elif method == "lal":
        from lal import ResampleREAL8TimeSeries
        lal_timeseries = ts.to_lal()
        ResampleREAL8TimeSeries(lal_timeseries, float(1.0/sampling_frequency))
        from gwpy.timeseries import TimeSeries
        return TimeSeries(lal_timeseries.data.data, epoch=lal_timeseries.epoch, dt=lal_timeserires.deltaT)
    else:
        raise ValueError(f"Unknown resampling method '{method}'; only 'gwpy' and 'lal' are supported")

def load_and_crop_strain(event_name):
    info = data_lvk._get_lvk_info_individual(event_name)
    data = data_lvk.get_lvk_strain_individual_sync(event)
    meta_data = read_meta_data_from_posterior(event_name)
    # Crop timeseries
    start_time = meta_data['start_time'][0]
    duration = meta_data['duration'][0]
    end_time = start_time + duration
    data_cropped = { ifo: strain.crop(start_time, end_time) for ifo, strain in data.items() }
    sampling_frequency = meta_data['sampling_frequency'][0]
    # Resample the time series
    data_cropped_resampled = { ifo: _resample_gwpy_timeseries(strain, sampling_frequency, method="lal") for ifo, strain in data_cropped.items() }
    return data_cropped_resampled

def build_interferometers(event_name):
    from bilby.gw.detector import InterferometerList
    # Load config, data, psd, and calibration
    info = data_lvk._get_lvk_info_individual(event_name)
    config = read_config_from_posterior(event_name)
    meta_data = read_meta_data_from_posterior(event_name)
    minimum_frequencies, maximum_frequencies = read_frequency_cuts_from_posterior(event_name)
    psds = data_lvk._process_psd_official(event_name)
    calibration_envelope, calibration_model, spline_calibration_nodes = read_calibration_envelope_from_posterior(event_name)
    # Create ifos
    detector_names = info['detectors']
    interferometers = InterferometerList(detector_names)
    for ifo in interferometers:
        ifo.minimum_frequency = minimum_frequencies[ifo.name]
        ifo.maximum_frequency = maximum_frequencies[ifo.name]
        ifo.set_strain_data_from_gwpy_timeseries(data[ifo.name])
        tukey_roll_off = float(config['tukey_roll_off'][0])
        ifo.strain_data.roll_off = tukey_roll_off
        f_ifo, psd_ifo = psds[ifo.name]
        ifo.power_spectral_density = bilby.gw.detector.PowerSpectralDensity( frequency_array=f_ifo, psd_array=psd_ifo)
        if calibration_model == "CubicSpline":
            ifo.calibration_model = bilby.gw.calibration.CubicSpline(
                prefix=f"recalib_{ifo.name}_",
                minimum_frequency=ifo.minimum_frequency,
                maximum_frequency=ifo.maximum_frequency,
                n_points=spline_calibration_nodes,
            )
            if ifo.name in calibration_envelope_dict:
                ifo.meta_data["calibration_envelope"] = np.asarray(
                    calibration_envelope_dict[ifo.name]
                )
        elif calibration_model is not None:
            raise ValueError(f"Unsupported calibration model: {calibration_model}")
    return interferometers

def build_waveform_generator(event_name):
    from bilby.gw.waveform_generator import WaveformGenerator
    config = read_config_from_posterior(event_name)
    meta_data = read_meta_data_from_posterior(event_name)
    minimum_frequencies, maximum_frequencies = read_frequency_cuts_from_posterior(event_name)
    duration = meta_data['duration'][0]
    sampling_frequency = meta_data['sampling_frequency'][0]
    waveform_approximant = config['waveform_approximant'][0]
    waveform_minimum_frequency = minimum_frequencies['waveform']
    reference_frequency = float(config['reference_frequency'][0])
    waveform_generator =  bilby.gw.WaveformGenerator( duration=duration, sampling_frequency=sampling_frequency, frequency_domain_source_model=bilby.gw.source.lal_binary_black_hole, waveform_arguments={ "waveform_approximant": waveform_approximant, "minimum_frequency": waveform_minimum_frequency, "reference_frequency": reference_frequency})
    return waveform_generator

def build_likelihood(event_name):
    info = data_lvk._get_lvk_info_individual(event_name)
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    config = read_config_from_posterior(event_name)
    psds = data_lvk._process_psd_official(event_name)
    prior = read_prior_from_posterior(event_name)
    interferometers = build_interferometers(event_name)
    waveform_generator = build_waveform_generator(event_name)
    jitter_time = bool(config['jitter_time'][0])
    reference_frame = config['reference_frame'][0]
    time_reference = config['time_reference'][0]
    likelihood = bilby.gw.GravitationalWaveTransient(
        interferometers=interferometers,
        waveform_generator=waveform_generator,
        jitter_time=settings['jitter_time'],
        time_marginalization=False,
        phase_marginalization=False,
        distance_marginalization=False,
        priors=priors,
        reference_frame=reference_frame,
        time_reference=time_reference,
    )
    return likelihood

if __name__ == "__main__":
    event_name = "GW231123_135430"
    info = data_lvk._get_lvk_info_individual(event_name)
    posterior_filename = data_lvk._get_lvk_pe_data_filename(event_name)
    print(f"Posterior filename for event '{event_name}': {posterior_filename}")
    config = read_config_from_posterior(event_name)
    psds = data_lvk._process_psd_official(event_name)
    calibration_envelope, calibration_model, spline_calibration_nodes = read_calibration_envelope_from_posterior(event_name)
    prior = read_prior_from_posterior(event_name)
    data = data_lvk.get_lvk_strain_individual_sync(event_name)
    data_cropped = load_and_crop_strain(event_name)
    interferometers = build_interferometers(event_name)
    waveform_generator = build_waveform_generator(event_name)
    likelihood = build_likelihood(event_name)



