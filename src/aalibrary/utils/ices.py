from echopype.utils.io import save_file, validate_output_path
from echopype.utils.coding import COMPRESSION_SETTINGS
import netCDF4
import xarray as xr
import numpy as np


def ragged_data_type_ices(echodata, variable_name: str = "") -> np.ndarray:
    """Transforms a gridded 4 dimensional variable from an Echodata object
    into a ragged array representation.

    Args:
    echodata (echopype.Echodata): Echopype echodata object containing a variable in the Beam_group1.
    variable_name (str): The name of the variable that needs to be transformed to
    a ragged array representation.

    Returns:
    ICES complain np array of type object.
    """

    num_pings = echodata["Sonar/Beam_group1"].sizes["ping_time"]
    num_channels = echodata["Sonar/Beam_group1"].sizes["channel"]
    num_beam = echodata["Sonar/Beam_group1"].sizes["beam"]

    compliant_np = np.empty((num_pings, num_channels, num_beam), object)

    for c, channel in enumerate(
        echodata["Sonar/Beam_group1"][variable_name].coords["channel"].values
    ):

        test = echodata["Sonar/Beam_group1"][variable_name].sel(channel=channel)

        # Find the first index along 'range_sample' where all values are NaN across 'beam'
        is_nan_across_beam = test.isnull().all(dim="beam")

        # Find the first index along 'range_sample' where 'is_nan_across_beam' is True
        first_nan_range_sample_indices = xr.apply_ufunc(
            np.argmax,
            is_nan_across_beam,
            input_core_dims=[["range_sample"]],
            exclude_dims=set(("range_sample",)),
            vectorize=True,  # Apply the function row-wise for each ping_time
            dask="parallelized",
            output_dtypes=[int],
        )

        found_nan_block_mask = is_nan_across_beam.isel(
            range_sample=first_nan_range_sample_indices.clip(min=0)
        )

        sample_t = []

        # Iterate through ping_time to populate sample_t
        for i, _ in enumerate(test["ping_time"].values):
            if found_nan_block_mask.isel(ping_time=i):
                value_to_append = (
                    test["range_sample"].values[
                        first_nan_range_sample_indices.isel(ping_time=i).item()
                    ]
                    - 1
                )
                sample_t.append(value_to_append)
            else:
                # If no all-NaN block was found, append the last range_sample index
                sample_t.append(test["range_sample"].values[-1])
        sample_t = np.array(sample_t)

        all_ping_segments = []

        for i, ping_da in enumerate(test):
            segment = ping_da.isel(range_sample=slice(sample_t[i])).values.transpose()
            all_ping_segments.append(segment)

        for i in range(len(compliant_np)):
            for j in range(4):
                compliant_np[i, c, j] = all_ping_segments[i][j].astype(np.float32)

    return compliant_np


def correct_dimensions_ices(echodata, variable_name: str = "") -> np.ndarray:
    """Extracts angle data from echopype DataArray.

    Args:
    echodata (echopype.DataArray): Echopype echodata object containing data.
    variable_name (str): The name of the variable that needs to be transformed to
    a ragged array representation.

    Returns:
    np.array that returns array with correct dimension as specified by ICES netcdf convention.
    """
    num_pings = echodata["Sonar/Beam_group1"].sizes["ping_time"]
    num_channels = echodata["Sonar/Beam_group1"].sizes["channel"]

    compliant_np = np.empty((num_pings, num_channels))

    for ping_time_val in range(num_pings):
        compliant_np[ping_time_val, :] = (
            echodata["Sonar/Beam_group1"][variable_name]
            .values.transpose()
            .astype(np.float32)
        )

    return compliant_np


def write_ek80_beamgroup_to_netcdf(echodata, export_file):
    """Writes echodata Beam_group ds to a Beam_groupX netcdf file.

    Args:
    echodata (echopype.echodata): Echopype echodata object containing beam_group_data.
    (echopype.DataArray): Echopype DataArray to be written.
    export_file (str or Path): Path to the NetCDF file.
    """
    ragged_backscatter_r_data = ragged_data_type_ices(echodata, "backscatter_r")
    ragged_backscatter_i_data = ragged_data_type_ices(echodata, "backscatter_i")
    beamwidth_receive_major_data = correct_dimensions_ices(
        echodata, "beamwidth_twoway_athwartship"
    )
    beamwidth_receive_minor_data = correct_dimensions_ices(
        echodata, "beamwidth_twoway_alongship"
    )
    echoangle_major_data = correct_dimensions_ices(echodata, "angle_offset_athwartship")
    echoangle_minor_data = correct_dimensions_ices(echodata, "angle_offset_alongship")
    equivalent_beam_angle_data = correct_dimensions_ices(
        echodata, "equivalent_beam_angle"
    )
    rx_beam_rotation_phi_data = (
        correct_dimensions_ices(echodata, "angle_offset_athwartship") * -1
    )
    rx_beam_rotation_psi_data = np.zeros(
        (echodata["Sonar/Beam_group1"].sizes["ping_time"], 1)
    )
    rx_beam_rotation_theta_data = correct_dimensions_ices(
        echodata, "angle_offset_alongship"
    )

    for i in range(echodata["Sonar/Beam_group1"].sizes["channel"]):

        with netCDF4.Dataset(export_file, "a", format="netcdf4") as ncfile:
            grp = ncfile.createGroup(f"Sonar/Beam_group{i+1}")
            grp.setncattr("beam_mode", echodata["Sonar/Beam_group1"].attrs["beam_mode"])
            grp.setncattr(
                "conversion_equation_type",
                echodata["Sonar/Beam_group1"].attrs["conversion_equation_t"],
            )
            grp.setncattr(
                "long_name", echodata["Sonar/Beam_group1"].coords["channel"].values[i]
            )

            # Create the VLEN type for 32-bit floats
            sample_t = grp.createVLType(np.float32, "sample_t")

            # Create ping_time dimension and ping_time coordinate variable
            grp.createDimension("ping_time", None)

            ping_time_var = grp.createVariable("ping_time", np.int64, ("ping_time",))
            ping_time_var.units = "nanoseconds since 1970-01-01 00:00:00Z"
            ping_time_var.standard_name = "time"
            ping_time_var.long_name = "Time-stamp of each ping"
            ping_time_var.axis = "T"
            ping_time_var.calendar = "gregorian"
            ping_time_var[:] = echodata["Sonar/Beam_group1"].coords[
                "ping_time"
            ].values - np.datetime64("1970-01-01T00:00:00Z")

            # Create beam dimension and coordinate variable
            grp.createDimension("beam", 1)

            beam_var = grp.createVariable("beam", "S1", ("beam",))
            beam_var.long_name = "Beam name"
            beam_var[:] = echodata["Sonar/Beam_group1"].coords["channel"].values[i]

            # Create beam dimension and coordinate variable
            grp.createDimension("sub_beam", 4)

            sub_beam_var = grp.createVariable("sub_beam", np.int64, ("sub_beam",))
            sub_beam_var.long_name = "Beam quadrant number"
            sub_beam_var[:] = echodata["Sonar/Beam_group1"].coords["beam"].values

            # Create backscatter_r variable
            backscatter_r = grp.createVariable(
                "backscatter_r",
                sample_t,
                ("ping_time", "beam", "sub_beam"),
            )
            backscatter_r[:] = ragged_backscatter_r_data[:, i, :]
            backscatter_r.setncattr(
                "long_name", "Raw backscatter measurements (real part)"
            )
            backscatter_r.units = "dB"

            # Create backscatter_i variable
            backscatter_i = grp.createVariable(
                "backscatter_i", sample_t, ("ping_time", "beam", "sub_beam")
            )
            backscatter_i[:] = ragged_backscatter_i_data[:, i, :].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"],
                1,
                echodata["Sonar/Beam_group1"].sizes["beam"],
            )
            backscatter_i.setncattr(
                "long_name", "Raw backscatter measurements (imaginary part)"
            )
            backscatter_i.units = "dB"

            # Create beam_stabilisation variable
            beam_stablisation = grp.createVariable(
                "beam_stablisation", int, ("ping_time", "beam")
            )
            beam_stablisation[:] = np.zeros(
                (echodata["Sonar/Beam_group1"].sizes["ping_time"], 1)
            )
            beam_stablisation.setncattr(
                "long_name", "Beam stabilisation applied(or not)"
            )

            # Create beam_type variable
            beam_type = grp.createVariable("beam_type", int, ())
            beam_type[:] = echodata["Sonar/Beam_group1"]["beam_type"].values[i]
            beam_type.setncattr("long_name", "type of transducer (0-single, 1-split)")

            # Create beamwidth_receive_major variable
            beamwidth_receive_major = grp.createVariable(
                "beamwidth_receive_major", np.float32, ("ping_time", "beam")
            )
            beamwidth_receive_major[:] = beamwidth_receive_major_data[:, i]
            beamwidth_receive_major.setncattr(
                "long_name",
                "Half power one-way receive beam width along major (horizontal) axis of beam",
            )
            beamwidth_receive_major.units = "arc_degree"
            beamwidth_receive_major.valid_range = [0.0, 360.0]

            # stopped here
            # Create beamwidth_receive_minor variable
            beamwidth_receive_minor = grp.createVariable(
                "beamwidth_receive_minor", np.float32, ("ping_time", "beam")
            )
            beamwidth_receive_minor[:] = beamwidth_receive_minor_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            beamwidth_receive_minor.setncattr(
                "long_name",
                "Half power one-way receive beam width along minor (vertical) axis of beam",
            )
            beamwidth_receive_minor.units = "arc_degree"
            beamwidth_receive_minor.valid_range = [0.0, 360.0]

            beamwidth_transmit_major = grp.createVariable(
                "beamwidth_transmit_major", np.float32, ("ping_time", "beam")
            )
            # Create beamwidth_transmit_major variable
            beamwidth_transmit_major[:] = beamwidth_receive_major_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            beamwidth_transmit_major.setncattr(
                "long_name",
                "Half power one-way receive beam width along major (horizontal) axis of beam",
            )
            beamwidth_transmit_major.units = "arc_degree"
            beamwidth_transmit_major.valid_range = [0.0, 360.0]

            # Create beamwidth_transmit_minor variable
            beamwidth_transmit_minor = grp.createVariable(
                "beamwidth_transmit_minor", np.float32, ("ping_time", "beam")
            )
            beamwidth_transmit_minor[:] = beamwidth_receive_minor_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            beamwidth_transmit_minor.setncattr(
                "long_name",
                "Half power one-way receive beam width along minor (vertical) axis of beam",
            )
            beamwidth_transmit_minor.units = "arc_degree"
            beamwidth_transmit_minor.valid_range = [0.0, 360.0]

            # Create blanking_interval variable
            blanking_interval = grp.createVariable(
                "blanking_interval", np.float32, ("ping_time", "beam")
            )
            blanking_interval[:] = np.zeros(
                (echodata["Sonar/Beam_group1"].sizes["ping_time"], 1)
            )
            blanking_interval.setncattr(
                "long_name", "Beam stabilisation applied(or not)"
            )
            blanking_interval.units = "s"
            blanking_interval.valid_min = 0.0

            # Create calibrated_frequency variable
            calibrated_frequency = grp.createVariable(
                "calibrated_frequency", np.float64, ()
            )
            calibrated_frequency[:] = echodata["Sonar/Beam_group1"][
                "frequency_nominal"
            ].values[i]
            calibrated_frequency.setncattr("long_name", "Calibration gain frequencies")
            calibrated_frequency.units = "Hz"
            calibrated_frequency.valid_min = 0.0

            # Create echoangle_major variable (talk to joe about this)
            echoangle_major = grp.createVariable(
                "echoangle_major", np.float32, ("ping_time", "beam")
            )
            echoangle_major[:] = echoangle_major_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            echoangle_major.setncattr(
                "long_name", "Echo arrival angle in the major beam coordinate"
            )
            echoangle_major.units = "arc_degree"
            echoangle_major.valid_range = [-180.0, 180.0]

            # Create echoangle_minor variable
            echoangle_minor = grp.createVariable(
                "echoangle_minor", np.float32, ("ping_time", "beam")
            )
            echoangle_minor[:] = echoangle_minor_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            echoangle_minor.setncattr(
                "long_name", "Echo arrival angle in the minor beam coordinate"
            )
            echoangle_minor.units = "arc_degree"
            echoangle_minor.valid_range = [-180.0, 180.0]

            # Create echoangle_major sensitivity variable
            echoangle_major_sensitivity = grp.createVariable(
                "echoangle_major_sensitivityr", np.float64, ()
            )
            echoangle_major_sensitivity[:] = echodata["Sonar/Beam_group1"][
                "angle_sensitivity_athwartship"
            ].values[i]
            echoangle_major_sensitivity.setncattr(
                "long_name", "Major angle scaling factor"
            )
            echoangle_major_sensitivity.units = "1"
            echoangle_major_sensitivity.valid_min = 0.0

            # Create echoangle_minor sensitivity variable
            echoangle_minor_sensitivity = grp.createVariable(
                "echoangle_minor_sensitivity", np.float64, ()
            )
            echoangle_minor_sensitivity[:] = echodata["Sonar/Beam_group1"][
                "angle_sensitivity_alongship"
            ].values[i]
            echoangle_minor_sensitivity.setncattr(
                "long_name", "Minor angle scaling factor"
            )
            echoangle_minor_sensitivity.units = "1"
            echoangle_minor_sensitivity.valid_min = 0.0

            # Create equivalent_beam_angle variable (weird angle values)
            equivalent_beam_angle = grp.createVariable(
                "equivalent_beam_angle", np.float32, ("ping_time", "beam")
            )
            equivalent_beam_angle[:] = equivalent_beam_angle_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            equivalent_beam_angle.setncattr("long_name", "Equivalent beam angle")

            # Create frequency variable
            frequency = grp.createVariable("frequency", np.float64, ())
            frequency[:] = echodata["Sonar/Beam_group1"]["frequency_nominal"].values[i]
            frequency.setncattr("long_name", "Calibration gain frequencies")
            frequency.units = "Hz"
            frequency.valid_min = 0.0

            # Create non_quantitative_processing variable
            non_quantitative_processing = grp.createVariable(
                "non_quantitative_processing", int, ("ping_time")
            )
            non_quantitative_processing[:] = np.zeros(
                echodata["Sonar/Beam_group1"].sizes["ping_time"]
            )
            non_quantitative_processing.setncattr(
                "long_name",
                "Presence or not of non-quantitative processing applied to the backscattering data (sonar specific)",
            )

            # Create platform_heading variable
            platform_heading = grp.createVariable(
                "platform_heading", np.float32, ("ping_time")
            )
            platform_heading[:] = echodata["Platform"]["heading"].values
            platform_heading.setncattr("long_name", "Platform heading(true)")
            platform_heading.units = "degrees_north"
            platform_heading.valid_range = [0, 360.0]

            # Create platform_latitude variable
            platform_latitude = grp.createVariable(
                "platform_latitude", np.float32, ("ping_time")
            )
            platform_latitude[:] = echodata["Platform"]["latitude"].interp(
                time1=echodata["Platform"].coords["time2"].values, method="nearest"
            )
            platform_latitude.setncattr(
                "long_name", "Heading of the platform at time of the ping"
            )
            platform_latitude.units = "degrees_north"
            platform_latitude.valid_range = [-180.0, 180.0]

            # Create platform_longitude variable
            platform_longitude = grp.createVariable(
                "platform_longitude", np.float64, ("ping_time")
            )
            platform_longitude[:] = echodata["Platform"]["longitude"].interp(
                time1=echodata["Platform"].coords["time2"].values, method="nearest"
            )
            platform_longitude.setncattr("long_name", "longitude")
            platform_longitude.units = "degrees_east"
            platform_longitude.valid_range = [-180.0, 180.0]

            # Create platform_pitch variable
            platform_pitch = grp.createVariable(
                "platform_pitch", np.float64, ("ping_time")
            )
            platform_pitch[:] = echodata["Platform"]["pitch"].values
            platform_pitch.setncattr("long_name", "pitch_angle")
            platform_pitch.units = "arc_degree"
            platform_pitch.valid_range = [-90.0, 90.0]

            # Create platform_roll variable
            platform_roll = grp.createVariable(
                "platform_roll", np.float64, ("ping_time")
            )
            platform_roll[:] = echodata["Platform"]["roll"].values
            platform_roll.setncattr("long_name", "roll angle")
            platform_roll.units = "arc_degree"

            # Create platform_vertical_offset variable
            platform_vertical_offset = grp.createVariable(
                "platform_vertical_offset", np.float64, ("ping_time")
            )
            platform_vertical_offset[:] = echodata["Platform"]["vertical_offset"].values
            platform_vertical_offset.setncattr(
                "long_name",
                "Platform vertical distance from reference point to the water line",
            )
            platform_vertical_offset.units = "m"

            # Create rx_beam_rotation_phi variable
            rx_beam_rotation_phi = grp.createVariable(
                "rx_beam_rotation_phi", np.float32, ("ping_time", "beam")
            )
            rx_beam_rotation_phi[:] = rx_beam_rotation_phi_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            rx_beam_rotation_phi.setncattr(
                "long_name", "receive beam angular rotation about the x axis"
            )
            rx_beam_rotation_phi.units = "arc_degree"
            rx_beam_rotation_phi.valid_range = [-180.0, 180.0]

            # Create rx_beam_rotation_psi variable
            rx_beam_rotation_psi = grp.createVariable(
                "rx_beam_rotation_psi", np.float32, ("ping_time", "beam")
            )
            rx_beam_rotation_psi[:] = rx_beam_rotation_psi_data
            rx_beam_rotation_psi.setncattr(
                "long_name", "receive beam angular rotation about the z axis"
            )
            rx_beam_rotation_psi.units = "arc_degree"
            rx_beam_rotation_psi.valid_range = [-180.0, 180.0]

            # Create rx_beam_rotation_theta variable
            rx_beam_rotation_theta = grp.createVariable(
                "rx_beam_roation_theta", np.float32, ("ping_time", "beam")
            )
            rx_beam_rotation_theta[:] = rx_beam_rotation_theta_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            rx_beam_rotation_theta.setncattr(
                "long_name", "receive beam angular rotation about the y axis"
            )
            rx_beam_rotation_theta.units = "arc_degree"
            rx_beam_rotation_theta.valid_range = [-90.0, 90.0]

            # Create sample_interval variable
            sample_interval = grp.createVariable(
                "sample_interval", np.float64, ("ping_time", "beam")
            )
            sample_interval[:] = (
                echodata["Sonar/Beam_group1"]["sample_interval"]
                .transpose()
                .values[:, i]
            )
            sample_interval.setncattr("long_name", "Equivalent beam angle")
            sample_interval.units = "s"
            sample_interval.valid_min = 0.0
            sample_interval.coordinates = (
                "ping_time platform_latitude platform_longitude"
            )

            # Create sample_time_offset variable
            sample_time_offset = grp.createVariable(
                "sample_time_offset", np.float32, ("ping_time", "beam")
            )
            sample_time_offset[:] = (
                echodata["Sonar/Beam_group1"]["sample_time_offset"]
                .transpose()
                .values[:, i]
            )
            sample_time_offset.setncattr(
                "long_name",
                "Time offset that is subtracted from the timestamp of each sample",
            )
            sample_time_offset.units = "s"

            # Create transmit_duration_nominal variable
            transmit_duration_nominal = grp.createVariable(
                "transmit_duration_nominal", np.float32, ("ping_time", "beam")
            )
            transmit_duration_nominal[:] = (
                echodata["Sonar/Beam_group1"]["transmit_duration_nominal"]
                .transpose()
                .values[:, i]
                .astype(np.float32)
            )
            transmit_duration_nominal.setncattr(
                "long_name", "Nominal duration of transmitted pulse"
            )
            transmit_duration_nominal.units = "Hz"
            transmit_duration_nominal.valid_min = 0.0

            # Create transmit_frequency_start variable
            transmit_frequency_start = grp.createVariable(
                "transmit_frequency_start", np.float32, ("ping_time", "beam")
            )
            transmit_frequency_start[:] = (
                echodata["Sonar/Beam_group1"]["transmit_frequency_start"]
                .transpose()
                .values[:, i]
                .astype(np.float32)
            )
            transmit_frequency_start.setncattr(
                "long_name", "Start frequency in transmitted pulse"
            )
            transmit_frequency_start.units = "Hz"
            transmit_frequency_start.valid_min = 0.0

            # Create transmit_frequency_stop variable
            transmit_frequency_stop = grp.createVariable(
                "transmit_frequency_stop", np.float32, ("ping_time", "beam")
            )
            transmit_frequency_stop[:] = (
                echodata["Sonar/Beam_group1"]["transmit_frequency_stop"]
                .transpose()
                .values[:, i]
                .astype(np.float32)
            )
            transmit_frequency_stop.setncattr(
                "long_name", "Stop frequency in transmitted pulse"
            )
            transmit_frequency_stop.units = "Hz"
            transmit_frequency_stop.valid_min = 0.0

            # Create transmit_power variable
            transmit_power = grp.createVariable(
                "transmit_power", np.float32, ("ping_time", "beam")
            )
            transmit_power[:] = (
                echodata["Sonar/Beam_group1"]["transmit_power"]
                .transpose()
                .values[:, i]
                .astype(np.float32)
            )
            transmit_power.setncattr("long_name", "Nominal transmit power")
            transmit_power.units = "W"
            transmit_power.valid_min = 0.0

            # Create transmit_type
            transmit_type = grp.createVariable(
                "transmit_type", np.float32, ("ping_time", "beam")
            )
            transmit_type[:] = (
                echodata["Sonar/Beam_group1"]["transmit_type"]
                .where(echodata["Sonar/Beam_group1"]["transmit_type"] != "CW", 0)
                .where(echodata["Sonar/Beam_group1"]["transmit_type"] != "LFM", 1)
                .transpose()
                .values[:, i]
                .astype(np.float32)
            )
            transmit_type.setncattr("long_name", "Type of transmitted pulse")

            # Create tx_beam_rotation_phi variable
            tx_beam_roation_phi = grp.createVariable(
                "tx_beam_roation_phi", np.float32, ("ping_time", "beam")
            )
            tx_beam_roation_phi[:] = rx_beam_rotation_phi_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            tx_beam_roation_phi.setncattr(
                "long_name", "receive beam angular rotation about the x axis"
            )
            tx_beam_roation_phi.units = "arc_degree"
            tx_beam_roation_phi.valid_range = [-180.0, 180.0]

            # Create rx_beam_rotation_psi variable
            tx_beam_roation_psi = grp.createVariable(
                "tx_beam_roation_psi", np.float32, ("ping_time", "beam")
            )
            tx_beam_roation_psi[:] = rx_beam_rotation_psi_data
            tx_beam_roation_psi.setncattr(
                "long_name", "receive beam angular rotation about the z axis"
            )
            tx_beam_roation_psi.units = "arc_degree"
            tx_beam_roation_psi.valid_range = [-180.0, 180.0]

            # Create rx_beam_rotation_theta variable
            tx_beam_roation_theta = grp.createVariable(
                "tx_beam_roation_theta", np.float32, ("ping_time", "beam")
            )
            tx_beam_roation_theta[:] = rx_beam_rotation_theta_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            tx_beam_roation_theta.setncattr(
                "long_name", "receive beam angular rotation about the y axis"
            )
            tx_beam_roation_theta.units = "arc_degree"
            tx_beam_roation_theta.valid_range = [-90.0, 90.0]


def write_ek60_beamgroup_to_netcdf(echodata, export_file):
    """
    Writes echopype Beam_group ds to a Beam_groupX netcdf file.

    Parameters:
    ed (echopype.DataArray): Echopype DataArray to be written.
    export_file (str or Path): Path to the output NetCDF file.
    """
    ragged_backscatter_r_data = ragged_data_type_ices(echodata, "backscatter_r")
    beamwidth_receive_major_data = correct_dimensions_ices(
        echodata, "beamwidth_twoway_athwartship"
    )
    beamwidth_receive_minor_data = correct_dimensions_ices(
        echodata, "beamwidth_twoway_alongship"
    )
    echoangle_major_data = ragged_data_type_ices(echodata, "angle_athwartship")
    echoangle_minor_data = ragged_data_type_ices(echodata, "angle_alongship")
    equivalent_beam_angle_data = correct_dimensions_ices(
        echodata, "equivalent_beam_angle"
    )
    rx_beam_rotation_phi_data = (
        ragged_data_type_ices(echodata, "angle_athwartship") * -1
    )
    rx_beam_rotation_psi_data = np.zeros(
        (echodata["Sonar/Beam_group1"].sizes["ping_time"], 1)
    )
    rx_beam_rotation_theta_data = ragged_data_type_ices(echodata, "angle_alongship")

    for i in range(echodata["Sonar/Beam_group1"].sizes["channel"]):

        with netCDF4.Dataset(export_file, "a", format="netcdf4") as ncfile:
            grp = ncfile.createGroup(f"Sonar/Beam_group{i+1}")
            grp.setncattr("beam_mode", echodata["Sonar/Beam_group1"].attrs["beam_mode"])
            grp.setncattr(
                "conversion_equation_type",
                echodata["Sonar/Beam_group1"].attrs["conversion_equation_t"],
            )
            grp.setncattr(
                "long_name", echodata["Sonar/Beam_group1"].coords["channel"].values[i]
            )

            # Create the VLEN type for 32-bit floats
            sample_t = grp.createVLType(np.float32, "sample_t")
            angle_t = grp.createVLType(np.float32, "angle_t")

            # Create ping_time dimension and ping_time coordinate variable
            grp.createDimension("ping_time", None)

            ping_time_var = grp.createVariable("ping_time", np.int64, ("ping_time",))
            ping_time_var.units = "nanoseconds since 1970-01-01 00:00:00Z"
            ping_time_var.standard_name = "time"
            ping_time_var.long_name = "Time-stamp of each ping"
            ping_time_var.axis = "T"
            ping_time_var.calendar = "gregorian"
            ping_time_var[:] = echodata["Sonar/Beam_group1"].coords[
                "ping_time"
            ].values - np.datetime64("1970-01-01T00:00:00Z")

            # Create beam dimension and coordinate variable
            grp.createDimension("beam", 1)

            beam_var = grp.createVariable("beam", "S1", ("beam",))
            beam_var.long_name = "Beam name"
            beam_var[:] = echodata["Sonar/Beam_group1"].coords["channel"].values[i]

            # Create backscatter_r variable
            backscatter_r = grp.createVariable(
                "backscatter_r", sample_t, ("ping_time", "beam")
            )
            backscatter_r[:] = ragged_backscatter_r_data[:, i]
            backscatter_r.setncattr(
                "long_name", "Raw backscatter measurements (real part)"
            )
            backscatter_r.units = "dB"

            # Create beam_stabilisation variable
            beam_stablisation = grp.createVariable(
                "beam_stablisation", int, ("ping_time", "beam")
            )
            beam_stablisation[:] = np.zeros(
                (echodata["Sonar/Beam_group1"].sizes["ping_time"], 1)
            )
            beam_stablisation.setncattr(
                "long_name", "Beam stabilisation applied(or not)"
            )

            # Create beam_type variable
            beam_type = grp.createVariable("beam_type", int, ())
            beam_type[:] = echodata["Sonar/Beam_group1"]["beam_type"].values[i]
            beam_type.setncattr("long_name", "type of transducer (0-single, 1-split)")

            # Create beamwidth_receive_major variable
            beamwidth_receive_major = grp.createVariable(
                "beamwidth_receive_major", np.float32, ("ping_time", "beam")
            )
            beamwidth_receive_major[:] = beamwidth_receive_major_data[:, i]
            beamwidth_receive_major.setncattr(
                "long_name",
                "Half power one-way receive beam width along major (horizontal) axis of beam",
            )
            beamwidth_receive_major.units = "arc_degree"
            beamwidth_receive_major.valid_range = [0.0, 360.0]

            # Create beamwidth_receive_minor variable
            beamwidth_receive_minor = grp.createVariable(
                "beamwidth_receive_minor", np.float32, ("ping_time", "beam")
            )
            beamwidth_receive_minor[:] = beamwidth_receive_minor_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            beamwidth_receive_minor.setncattr(
                "long_name",
                "Half power one-way receive beam width along minor (vertical) axis of beam",
            )
            beamwidth_receive_minor.units = "arc_degree"
            beamwidth_receive_minor.valid_range = [0.0, 360.0]

            beamwidth_transmit_major = grp.createVariable(
                "beamwidth_transmit_major", np.float32, ("ping_time", "beam")
            )
            # Create beamwidth_transmit_major variable
            beamwidth_transmit_major[:] = beamwidth_receive_major_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            beamwidth_transmit_major.setncattr(
                "long_name",
                "Half power one-way receive beam width along major (horizontal) axis of beam",
            )
            beamwidth_transmit_major.units = "arc_degree"
            beamwidth_transmit_major.valid_range = [0.0, 360.0]

            # Create beamwidth_transmit_minor variable
            beamwidth_transmit_minor = grp.createVariable(
                "beamwidth_transmit_minor", np.float32, ("ping_time", "beam")
            )
            beamwidth_transmit_minor[:] = beamwidth_receive_minor_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            beamwidth_transmit_minor.setncattr(
                "long_name",
                "Half power one-way receive beam width along minor (vertical) axis of beam",
            )
            beamwidth_transmit_minor.units = "arc_degree"
            beamwidth_transmit_minor.valid_range = [0.0, 360.0]

            # Create blanking_interval variable
            blanking_interval = grp.createVariable(
                "blanking_interval", float, ("ping_time", "beam")
            )
            blanking_interval[:] = np.zeros(
                (echodata["Sonar/Beam_group1"].sizes["ping_time"], 1)
            )
            blanking_interval.setncattr(
                "long_name", "Beam stabilisation applied(or not)"
            )
            blanking_interval.units = "s"
            blanking_interval.valid_min = 0.0

            # Create calibrated_frequency variable
            calibrated_frequency = grp.createVariable(
                "calibrated_frequency", np.float64, ()
            )
            calibrated_frequency[:] = echodata["Sonar/Beam_group1"][
                "frequency_nominal"
            ].values[i]
            calibrated_frequency.setncattr("long_name", "Calibration gain frequencies")
            calibrated_frequency.units = "Hz"
            calibrated_frequency.valid_min = 0.0

            # Create echoangle_major variable (talk to joe about this)
            echoangle_major = grp.createVariable(
                "echoangle_major", angle_t, ("ping_time", "beam")
            )
            echoangle_major[:] = echoangle_major_data[:, i]
            echoangle_major.setncattr(
                "long_name", "Echo arrival angle in the major beam coordinate"
            )
            echoangle_major.units = "arc_degree"
            echoangle_major.valid_range = [-180.0, 180.0]

            # Create echoangle_minor variable
            echoangle_minor = grp.createVariable(
                "echoangle_minor", angle_t, ("ping_time", "beam")
            )
            echoangle_minor[:] = echoangle_minor_data[:, i]
            echoangle_minor.setncattr(
                "long_name", "Echo arrival angle in the minor beam coordinate"
            )
            echoangle_minor.units = "arc_degree"
            echoangle_minor.valid_range = [-180.0, 180.0]

            # Create echoangle_major sensitivity variable
            echoangle_major_sensitivity = grp.createVariable(
                "echoangle_major_sensitivityr", np.float64, ()
            )
            echoangle_major_sensitivity[:] = echodata["Sonar/Beam_group1"][
                "angle_sensitivity_athwartship"
            ].values[i]
            echoangle_major_sensitivity.setncattr(
                "long_name", "Major angle scaling factor"
            )
            echoangle_major_sensitivity.units = "1"
            echoangle_major_sensitivity.valid_min = 0.0

            # Create echoangle_minor sensitivity variable
            echoangle_minor_sensitivity = grp.createVariable(
                "echoangle_minor_sensitivity", np.float64, ()
            )
            echoangle_minor_sensitivity[:] = echodata["Sonar/Beam_group1"][
                "angle_sensitivity_alongship"
            ].values[i]
            echoangle_minor_sensitivity.setncattr(
                "long_name", "Minor angle scaling factor"
            )
            echoangle_minor_sensitivity.units = "1"
            echoangle_minor_sensitivity.valid_min = 0.0

            # Create equivalent_beam_angle variable (weird angle values)
            equivalent_beam_angle = grp.createVariable(
                "equivalent_beam_angle", np.float64, ("ping_time", "beam")
            )
            equivalent_beam_angle[:] = equivalent_beam_angle_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            equivalent_beam_angle.setncattr("long_name", "Equivalent beam angle")

            # Create frequency variable
            frequency = grp.createVariable("frequency", np.float64, ())
            frequency[:] = echodata["Sonar/Beam_group1"]["frequency_nominal"].values[i]
            frequency.setncattr("long_name", "Calibration gain frequencies")
            frequency.units = "Hz"
            frequency.valid_min = 0.0

            # Create non_quantitative_processing variable
            non_quantitative_processing = grp.createVariable(
                "non_quantitative_processing", int, ("ping_time")
            )
            non_quantitative_processing[:] = np.zeros(
                echodata["Sonar/Beam_group1"].sizes["ping_time"]
            )
            non_quantitative_processing.setncattr(
                "long_name",
                "Presence or not of non-quantitative processing applied to the backscattering data (sonar specific)",
            )

            # Create platoform_latitude variable
            platoform_latitude = grp.createVariable(
                "platoform_latitude", np.float64, ("ping_time")
            )
            platoform_latitude[:] = echodata["Platform"]["latitude"].interp(
                time1=echodata["Platform"].coords["time2"].values, method="nearest"
            )
            platoform_latitude.setncattr(
                "long_name", "Heading of the platform at time of the ping"
            )
            platoform_latitude.units = "degrees_north"
            platoform_latitude.valid_range = [-180.0, 180.0]

            # Create platoform_longitude variable
            platoform_longitude = grp.createVariable(
                "platoform_longitude", np.float64, ("ping_time")
            )
            platoform_longitude[:] = echodata["Platform"]["longitude"].interp(
                time1=echodata["Platform"].coords["time2"].values, method="nearest"
            )
            platoform_longitude.setncattr("long_name", "longitude")
            platoform_longitude.units = "degrees_east"
            platoform_longitude.valid_range = [-180.0, 180.0]

            # Create platoform_pitch variable
            platform_pitch = grp.createVariable(
                "platform_pitch", np.float64, ("ping_time")
            )
            platform_pitch[:] = echodata["Platform"]["pitch"].values
            platform_pitch.setncattr("long_name", "pitch_angle")
            platform_pitch.units = "arc_degree"
            platform_pitch.valid_range = [-90.0, 90.0]

            # Create platoform_roll variable
            platoform_roll = grp.createVariable(
                "platform_roll", np.float64, ("ping_time")
            )
            platoform_roll[:] = echodata["Platform"]["roll"].values
            platoform_roll.setncattr("long_name", "roll angle")
            platoform_roll.units = "arc_degree"

            # Create platoform_vertical_offset variable
            platoform_vertical_offset = grp.createVariable(
                "platoform_vertical_offset", np.float64, ("ping_time")
            )
            platoform_vertical_offset[:] = echodata["Platform"][
                "vertical_offset"
            ].values
            platoform_vertical_offset.setncattr(
                "long_name",
                "Platform vertical distance from reference point to the water line",
            )
            platoform_vertical_offset.units = "m"

            # Create rx_beam_rotation_phi variable
            rx_beam_rotation_phi = grp.createVariable(
                "rx_beam_rotation_phi", angle_t, ("ping_time", "beam")
            )
            rx_beam_rotation_phi[:] = rx_beam_rotation_phi_data[:, i]
            rx_beam_rotation_phi.setncattr(
                "long_name", "receive beam angular rotation about the x axis"
            )
            rx_beam_rotation_phi.units = "arc_degree"
            rx_beam_rotation_phi.valid_range = [-180.0, 180.0]

            # Create rx_beam_rotation_psi variable
            rx_beam_rotation_psi = grp.createVariable(
                "rx_beam_rotation_psi", np.float64, ("ping_time", "beam")
            )
            rx_beam_rotation_psi[:] = rx_beam_rotation_psi_data
            rx_beam_rotation_psi.setncattr(
                "long_name", "receive beam angular rotation about the z axis"
            )
            rx_beam_rotation_psi.units = "arc_degree"
            rx_beam_rotation_psi.valid_range = [-180.0, 180.0]

            # Create rx_beam_rotation_theta variable
            rx_beam_rotation_theta = grp.createVariable(
                "rx_beam_roation_theta", angle_t, ("ping_time", "beam")
            )
            rx_beam_rotation_theta[:] = rx_beam_rotation_theta_data[:, i]
            rx_beam_rotation_theta.setncattr(
                "long_name", "receive beam angular rotation about the y axis"
            )
            rx_beam_rotation_theta.units = "arc_degree"
            rx_beam_rotation_theta.valid_range = [-90.0, 90.0]

            # Create sample_interval variable
            sample_interval = grp.createVariable(
                "sample_interval", np.float64, ("ping_time", "beam")
            )
            sample_interval[:] = (
                echodata["Sonar/Beam_group1"]["sample_interval"]
                .transpose()
                .values[:, i]
            )
            sample_interval.setncattr("long_name", "Equivalent beam angle")
            sample_interval.units = "s"
            sample_interval.valid_min = 0.0
            sample_interval.coordinates = (
                "ping_time platform_latitude platform_longitude"
            )

            # Create sample_time_offset variable
            sample_time_offset = grp.createVariable(
                "sample_time_offset", np.float64, ("ping_time", "beam")
            )
            sample_time_offset[:] = (
                echodata["Sonar/Beam_group1"]["sample_time_offset"]
                .transpose()
                .values[:, i]
            )
            sample_time_offset.setncattr(
                "long_name",
                "Time offset that is subtracted from the timestamp of each sample",
            )
            sample_time_offset.units = "s"

            # Create transmit_duration_nominal variable
            transmit_duration_nominal = grp.createVariable(
                "transmit_duration_nominal", np.float64, ("ping_time", "beam")
            )
            transmit_duration_nominal[:] = (
                echodata["Sonar/Beam_group1"]["transmit_duration_nominal"]
                .transpose()
                .values[:, i]
            )
            transmit_duration_nominal.setncattr(
                "long_name", "Nominal duration of transmitted pulse"
            )
            transmit_duration_nominal.units = "Hz"
            transmit_duration_nominal.valid_min = 0.0

            # Create transmit_frequency_start variable
            transmit_frequency_start = grp.createVariable(
                "transmit_frequency_start", np.float64, ("ping_time")
            )
            transmit_frequency_start[:] = echodata["Sonar/Beam_group1"][
                "transmit_frequency_start"
            ].values[i]
            transmit_frequency_start.setncattr(
                "long_name", "Start frequency in transmitted pulse"
            )
            transmit_frequency_start.units = "Hz"
            transmit_frequency_start.valid_min = 0.0

            # Create transmit_frequency_stop variable
            transmit_frequency_stop = grp.createVariable(
                "transmit_frequency_stop", np.float64, ("ping_time")
            )
            transmit_frequency_stop[:] = echodata["Sonar/Beam_group1"][
                "transmit_frequency_stop"
            ].values[i]
            transmit_frequency_stop.setncattr(
                "long_name", "Stop frequency in transmitted pulse"
            )
            transmit_frequency_stop.units = "Hz"
            transmit_frequency_stop.valid_min = 0.0

            # Create transmit_power variable
            transmit_power = grp.createVariable(
                "transmit_power", np.float64, ("ping_time", "beam")
            )
            transmit_power[:] = (
                echodata["Sonar/Beam_group1"]["transmit_power"].transpose().values[:, i]
            )
            transmit_power.setncattr("long_name", "Nominal transmit power")
            transmit_power.units = "W"
            transmit_power.valid_min = 0.0

            # Create transmit_type
            transmit_type = grp.createVariable("transmit_type", np.float64, ())
            transmit_type[:] = 0
            transmit_type.setncattr("long_name", "Type of transmitted pulse")

            # Create tx_beam_rotation_phi variable
            tx_beam_roation_phi = grp.createVariable(
                "tx_beam_roation_phi", angle_t, ("ping_time", "beam")
            )
            tx_beam_roation_phi[:] = rx_beam_rotation_phi_data[:, i].reshape(
                echodata["Sonar/Beam_group1"].sizes["ping_time"], 1
            )
            tx_beam_roation_phi.setncattr(
                "long_name", "receive beam angular rotation about the x axis"
            )
            tx_beam_roation_phi.units = "arc_degree"
            tx_beam_roation_phi.valid_range = [-180.0, 180.0]

            # Create rx_beam_rotation_psi variable
            tx_beam_roation_psi = grp.createVariable(
                "tx_beam_roation_psi", np.float32, ("ping_time", "beam")
            )
            tx_beam_roation_psi[:] = rx_beam_rotation_psi_data
            tx_beam_roation_psi.setncattr(
                "long_name", "receive beam angular rotation about the z axis"
            )
            tx_beam_roation_psi.units = "arc_degree"
            tx_beam_roation_psi.valid_range = [-180.0, 180.0]

            # Create rx_beam_rotation_theta variable
            tx_beam_roation_theta = grp.createVariable(
                "tx_beam_roation_theta", angle_t, ("ping_time", "beam")
            )
            tx_beam_roation_theta[:] = rx_beam_rotation_theta_data[:, i]
            tx_beam_roation_theta.setncattr(
                "long_name", "receive beam angular rotation about the y axis"
            )
            tx_beam_roation_theta.units = "arc_degree"
            tx_beam_roation_theta.valid_range = [-90.0, 90.0]


def echopype_ek60_raw_to_ices_netcdf(echodata, export_file):
    """Writes echodata Beam_group ds to a Beam_groupX netcdf file.

    Args:
    echodata (echopype.echodata): Echopype echodata object containing beam_group_data.
    (echopype.DataArray): Echopype DataArray to be written.
    export_file (str or Path): Path to the NetCDF file.
    """

    engine = "netcdf4"

    output_file = validate_output_path(
        source_file=echodata.source_file,
        engine=engine,
        save_path=export_file,
        output_storage_options={},
    )

    save_file(
        echodata["Top-level"],
        path=output_file,
        mode="w",
        engine=engine,
        compression_settings=COMPRESSION_SETTINGS[engine],
    )
    save_file(
        echodata["Environment"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Environment",
        compression_settings=COMPRESSION_SETTINGS[engine],
    )
    save_file(
        echodata["Platform"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Platform",
        compression_settings=COMPRESSION_SETTINGS[engine],
    )

    save_file(
        echodata["Platform/NMEA"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Platform/NMEA",
        compression_settings=COMPRESSION_SETTINGS[engine],
    )

    save_file(
        echodata["Sonar"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Sonar",
        compression_settings=COMPRESSION_SETTINGS[engine],
    )

    echopype_ek60_raw_to_ices_netcdf(echodata, output_file)

    save_file(
        echodata["Vendor_specific"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Vendor_specific",
        compression_settings=COMPRESSION_SETTINGS[engine],
    )


def echopype_ek80_raw_to_ices_netcdf(echodata, export_file):
    """Writes echodata Beam_group ds to a Beam_groupX netcdf file.

    Args:
    echodata (echopype.echodata): Echopype echodata object containing beam_group_data.
    (echopype.DataArray): Echopype DataArray to be written.
    export_file (str or Path): Path to the NetCDF file.
    """
    engine = "netcdf4"

    output_file = validate_output_path(
        source_file=echodata.source_file,
        engine=engine,
        save_path=export_file,
        output_storage_options={},
    )
    
    save_file(
        echodata["Top-level"],
        path=output_file,
        mode="w",
        engine=engine,
        compression_settings=COMPRESSION_SETTINGS[engine]
    )
    save_file(
        echodata["Environment"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Environment",
        compression_settings=COMPRESSION_SETTINGS[engine]
    )
    save_file(
        echodata["Platform"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Platform",
        compression_settings=COMPRESSION_SETTINGS[engine]
    )
    save_file(
        echodata["Platform/NMEA"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Platform/NMEA",
        compression_settings=COMPRESSION_SETTINGS[engine]
    )
    save_file(
        echodata["Sonar"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Sonar",
        compression_settings=COMPRESSION_SETTINGS[engine]
    )
    write_ek80_beamgroup_to_netcdf(echodata, output_file)
    save_file(
        echodata["Vendor_specific"],
        path=output_file,
        mode="a",
        engine=engine,
        group="Vendor_specific",
        compression_settings=COMPRESSION_SETTINGS[engine]
    )
