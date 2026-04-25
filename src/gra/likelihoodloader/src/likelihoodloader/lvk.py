"""LVK-style Bilby likelihood construction from a PE posterior and strain GWFs."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping, Sequence
from typing import Any

import bilby
import bilby.gw
import h5py
import numpy as np
from gwpy.timeseries import TimeSeries

from likelihoodloader.io import (
    find_dictionary,
    h5_to_dict,
    load_posterior_dict,
    read_gwf_file,
    PRIORITY_WAVEFORMS,
)


def load_psds_from_posterior(
    posterior_path: str | os.PathLike,
    posterior_dict: dict[str, Any] | None = None,
) -> dict[str, np.ndarray]:
    d = posterior_dict
    if d is not None and "psds" in d:
        return d["psds"]  # type: ignore[no-any-return]
    with h5py.File(posterior_path, "r") as f:
        approx: str | None = None
        candidates = [f"C00:{w}" for w in PRIORITY_WAVEFORMS] + list(
            PRIORITY_WAVEFORMS
        )
        for a in candidates:
            if a in f:
                approx = a
                break
        if approx is None:
            raise KeyError(
                f"No known approximant in {posterior_path} for PSDs. "
                f"Keys: {list(f.keys())}"
            )
        pe = h5_to_dict(f[approx])
    if "psds" not in pe:
        raise KeyError(f"No 'psds' in approximant group {approx!r} of {posterior_path}")
    return pe["psds"]  # type: ignore[no-any-return]


def read_calibration_envelope_from_posterior(
    posterior_path: str | os.PathLike,
    posterior_dict: dict[str, Any] | None = None,
) -> tuple[dict, str, int]:
    d = posterior_dict or load_posterior_dict(posterior_path)
    calibration_envelope_path, calibration_envelope = find_dictionary(
        d, "calibration_envelope"
    )[0]
    calibration_model_path, calibration_model = find_dictionary(
        d, "calibration_model"
    )[0]
    if calibration_model[0] != "CubicSpline":
        raise ValueError(
            f"Expected calibration model to be 'CubicSpline', but got {calibration_model!r}"
        )
    _path, spline_calibration_nodes = find_dictionary(d, "spline_calibration_nodes")[
        0
    ]
    return (
        calibration_envelope,
        str(calibration_model[0]),
        int(spline_calibration_nodes[0]),
    )


def read_prior_from_posterior(
    posterior_path: str | os.PathLike,
    posterior_dict: dict[str, Any] | None = None,
) -> "bilby.core.prior.PriorDict":
    from astropy.cosmology import Planck18, LambdaCDM  # noqa: F401
    from bilby.core.prior import (  # noqa: F401
        Constraint,
        Cosine,
        DeltaFunction,
        Gaussian,
        JointPrior,
        LogUniform,
        PowerLaw,
        PriorDict,
        Sine,
        Uniform,
    )

    d = posterior_dict or load_posterior_dict(posterior_path)
    _p, prior_dict_unformatted = find_dictionary(d, "analytic")[0]
    prior_dict = {}
    for key in prior_dict_unformatted.keys():
        prior_dict[key] = eval(prior_dict_unformatted[key][0])  # noqa: S307
    return PriorDict(prior_dict)


def read_config_from_posterior(
    posterior_path: str | os.PathLike,
    posterior_dict: dict[str, Any] | None = None,
) -> dict[str, Any]:
    d = posterior_dict or load_posterior_dict(posterior_path)
    _path, config = find_dictionary(d, "config")[0]
    return config  # type: ignore[no-any-return]


def read_meta_data_from_posterior(
    posterior_path: str | os.PathLike,
    posterior_dict: dict[str, Any] | None = None,
) -> dict[str, Any]:
    d = posterior_dict or load_posterior_dict(posterior_path)
    return d["meta_data"]["meta_data"]  # type: ignore[no-any-return]


def read_frequency_cuts_from_posterior(
    posterior_path: str | os.PathLike,
    posterior_dict: dict[str, Any] | None = None,
) -> tuple[dict, dict]:
    def format_str(s: str) -> str:
        s = re.sub(r"\{(.*)\}", r"dict(\1)", s)
        return s.replace(":", "=")

    config = read_config_from_posterior(posterior_path, posterior_dict)
    minimum_frequency_dict = eval(format_str(config["minimum_frequency"][0]))  # noqa: S307
    maximum_frequency_dict = eval(format_str(config["maximum_frequency"][0]))  # noqa: S307
    return minimum_frequency_dict, maximum_frequency_dict


def _resample_gwpy_timeseries(
    ts: TimeSeries, sampling_frequency: float, resampling_method: str
) -> TimeSeries:
    if resampling_method == "gwpy":
        return ts.resample(sampling_frequency)
    if resampling_method == "lal":
        from lal import ResampleREAL8TimeSeries
        lal_timeseries = ts.to_lal()
        ResampleREAL8TimeSeries(lal_timeseries, float(1.0 / sampling_frequency))
        return TimeSeries(
            lal_timeseries.data.data,
            epoch=lal_timeseries.epoch,
            dt=lal_timeseries.deltaT,
        )
    raise ValueError(
        f"Unknown resampling resampling_method {resampling_method!r}; "
        "only 'gwpy' and 'lal' are supported"
    )


def load_strain_from_gwf_map(
    strain_gwf: Mapping[str, str | os.PathLike],
) -> dict[str, TimeSeries]:
    return {ifo: read_gwf_file(p) for ifo, p in strain_gwf.items()}


def load_and_crop_strain(
    posterior_path: str | os.PathLike,
    strain_gwf: Mapping[str, str | os.PathLike],
    *,
    posterior_dict: dict[str, Any] | None = None,
    resampling_method: str = "lal",
) -> dict[str, TimeSeries]:
    data = load_strain_from_gwf_map(strain_gwf)
    meta_data = read_meta_data_from_posterior(posterior_path, posterior_dict)
    start_time = meta_data["start_time"][0]
    duration = meta_data["duration"][0]
    end_time = start_time + duration
    data_cropped = {
        ifo: strain.crop(start_time, end_time) for ifo, strain in data.items()
    }
    sampling_frequency = meta_data["sampling_frequency"][0]
    return {
        ifo: _resample_gwpy_timeseries(
            strain, sampling_frequency, resampling_method=resampling_method
        )
        for ifo, strain in data_cropped.items()
    }


def build_interferometers(
    posterior_path: str | os.PathLike,
    data: Mapping[str, TimeSeries],
    *,
    psds: Mapping[str, np.ndarray] | None = None,
    posterior_dict: dict[str, Any] | None = None,
) -> "bilby.gw.detector.InterferometerList":
    from bilby.gw.detector import InterferometerList, PowerSpectralDensity

    path = os.fspath(posterior_path)
    d = posterior_dict or load_posterior_dict(path)
    config = read_config_from_posterior(path, d)
    minimum_frequencies, maximum_frequencies = read_frequency_cuts_from_posterior(
        path, d
    )
    if psds is None:
        psds = load_psds_from_posterior(path, d)
    calibration_envelope, calibration_model, spline_calibration_nodes = (
        read_calibration_envelope_from_posterior(path, d)
    )
    names = list(data.keys())
    if not names:
        raise ValueError("data must be non-empty (IFO -> TimeSeries)")

    interferometers = InterferometerList(names)
    for ifo in interferometers:
        ifo.minimum_frequency = minimum_frequencies[ifo.name]
        ifo.maximum_frequency = maximum_frequencies[ifo.name]
        ifo.set_strain_data_from_gwpy_timeseries(data[ifo.name])
        tukey_roll_off = float(config["tukey_roll_off"][0])
        ifo.strain_data.roll_off = tukey_roll_off
        f_ifo, psd_ifo = np.transpose(psds[ifo.name])
        ifo.power_spectral_density = PowerSpectralDensity(
            frequency_array=f_ifo, psd_array=psd_ifo
        )
        if calibration_model == "CubicSpline":
            ifo.calibration_model = bilby.gw.calibration.CubicSpline(
                prefix=f"recalib_{ifo.name}_",
                minimum_frequency=ifo.minimum_frequency,
                maximum_frequency=ifo.maximum_frequency,
                n_points=spline_calibration_nodes,
            )
            if ifo.name in calibration_envelope:
                ifo.meta_data["calibration_envelope"] = np.asarray(
                    calibration_envelope[ifo.name]
                )
        elif calibration_model is not None:
            raise ValueError(
                f"Unsupported calibration model: {calibration_model!r}"
            )
    return interferometers


def build_waveform_generator(
    posterior_path: str | os.PathLike,
    *,
    posterior_dict: dict[str, Any] | None = None,
) -> "bilby.gw.waveform_generator.WaveformGenerator":
    d = posterior_dict or load_posterior_dict(posterior_path)
    config = read_config_from_posterior(posterior_path, d)
    meta_data = read_meta_data_from_posterior(posterior_path, d)
    minimum_frequencies, _maximum_frequencies = read_frequency_cuts_from_posterior(
        posterior_path, d
    )
    duration = meta_data["duration"][0]
    sampling_frequency = meta_data["sampling_frequency"][0]
    waveform_approximant = config["waveform_approximant"][0]
    waveform_minimum_frequency = minimum_frequencies["waveform"]
    reference_frequency = float(config["reference_frequency"][0])
    return bilby.gw.WaveformGenerator(
        duration=duration,
        sampling_frequency=sampling_frequency,
        frequency_domain_source_model=bilby.gw.source.lal_binary_black_hole,
        waveform_arguments={
            "waveform_approximant": waveform_approximant,
            "minimum_frequency": waveform_minimum_frequency,
            "reference_frequency": reference_frequency,
        },
    )


def build_likelihood(
    posterior_path: str | os.PathLike,
    strain_gwf: Mapping[str, str | os.PathLike]
    | str
    | os.PathLike
    | Sequence[str | os.PathLike],
    *,
    psds: Mapping[str, np.ndarray] | None = None,
    resampling_method: str = "lal",
) -> "bilby.gw.GravitationalWaveTransient":
    path = os.fspath(posterior_path)
    posterior_dict = load_posterior_dict(path)
    gwf_map = _normalize_strain_gwf(strain_gwf, posterior_dict, path)
    data_cropped = load_and_crop_strain(
        path,
        gwf_map,
        posterior_dict=posterior_dict,
        resampling_method=resampling_method,
    )
    if psds is None:
        psds = load_psds_from_posterior(path, posterior_dict)
    priors = read_prior_from_posterior(path, posterior_dict)
    config = read_config_from_posterior(path, posterior_dict)
    interferometers = build_interferometers(
        path, data_cropped, psds=psds, posterior_dict=posterior_dict
    )
    waveform_generator = build_waveform_generator(
        path, posterior_dict=posterior_dict
    )
    jitter_time = bool(config["jitter_time"][0])
    reference_frame = config["reference_frame"][0]
    time_reference = config["time_reference"][0]
    return bilby.gw.GravitationalWaveTransient(
        interferometers=interferometers,
        waveform_generator=waveform_generator,
        jitter_time=jitter_time,
        time_marginalization=False,
        phase_marginalization=False,
        distance_marginalization=False,
        priors=priors,
        reference_frame=reference_frame,
        time_reference=time_reference,
    )


def _detector_ifos(
    posterior_path: str, posterior_dict: dict[str, Any]
) -> list[str]:
    m, _ = read_frequency_cuts_from_posterior(posterior_path, posterior_dict)
    return [k for k in m if k != "waveform"]


def _normalize_strain_gwf(
    strain_gwf: Mapping[str, str | os.PathLike]
    | str
    | os.PathLike
    | Sequence[str | os.PathLike],
    posterior_dict: dict[str, Any],
    posterior_path: str,
) -> dict[str, str]:
    if isinstance(strain_gwf, Mapping):
        return {str(k): os.fspath(v) for k, v in strain_gwf.items()}

    ifos = _detector_ifos(posterior_path, posterior_dict)

    if isinstance(strain_gwf, (str, os.PathLike)):
        p = os.fspath(strain_gwf)
        if len(ifos) == 1:
            return {ifos[0]: p}
        raise ValueError(
            f"One GWF path given but posterior lists {len(ifos)} detectors {ifos}. "
            "Pass dict[IFO, path]."
        )

    if isinstance(strain_gwf, (list, tuple)):
        paths = [os.fspath(x) for x in strain_gwf]
        if len(paths) != len(ifos):
            raise ValueError(
                f"Expected {len(ifos)} strain files (order: {ifos}), got {len(paths)}"
            )
        return dict(zip(ifos, paths, strict=True))

    raise TypeError(
        "strain_gwf must be dict[IFO, path], list/tuple of paths in minimum_frequency "
        "IFO order, or a single path for single-detector"
    )
