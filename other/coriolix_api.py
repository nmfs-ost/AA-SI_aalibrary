"""This file contains the functions necessary for interacting with the Coriolix
API, to obtain Coriolix Metadata for cruises."""

import json
import requests
from typing import List
from pprint import pprint


class CoriolixAPI:
    api_root_url = "https://coriolix.savannah.skio.uga.edu/api/"

    def __init__(self):
        self.get_api_root()

    def get_api_root(self):
        self.api_root = self._api_call_to_json(call=self.api_root_url)
        # pprint(self.api_root)

    def _api_call_to_json(self, call: str = "") -> dict:
        """Uses a GET method on the API using the formatted url `call`. Returns
        a JSON object.

        Args:
            call (str, optional): The formatted url to perform a GET method on.
                Defaults to "".

        Returns:
            dict: A dictionary composed of the content from the GET request.
        """

        content = json.loads(requests.get(url=call).content)
        return content

    def _format_instance_str(self, instance: str = ""):
        instance = instance.replace(" ", "%20")
        return instance

    def _format_instance_call(self, instance: str = "", call: str = ""):
        instance = self._format_instance_str(instance=instance)
        return self.api_root[call] + f"{instance}/"

    def get_alert_config_list(self) -> List:
        """Gets a list of all `alert_config` instances.

        Returns:
            List: A list containing dicts of all `alert_config` instances.
        """
        return self._api_call_to_json(call=self.api_root["alert_config"])

    def get_alert_action_list(self) -> List:
        """Gets a list of all `alert_action` instances.

        Returns:
            List: A list containing dicts of all `alert_action` instances.
        """
        return self._api_call_to_json(call=self.api_root["alert_action"])

    def get_alert_current_list(self) -> List:
        """Gets a list of all `alert_current` instances.

        Returns:
            List: A list containing dicts of all `alert_current` instances.
        """
        return self._api_call_to_json(call=self.api_root["alert_current"])

    def get_alert_archive_list(self) -> List:
        """Gets a list of all `alert_archive` instances.

        Returns:
            List: A list containing dicts of all `alert_archive` instances.
        """
        return self._api_call_to_json(call=self.api_root["alert_archive"])

    def get_channel_list(self) -> List:
        """Gets a list of all `channel` instances.

        Returns:
            List: A list containing dicts of all `channel` instances.
        """
        return self._api_call_to_json(call=self.api_root["channel"])

    def get_settings_list(self) -> List:
        """Gets a list of all `settings` instances.

        Returns:
            List: A list containing dicts of all `settings` instances.
        """
        return self._api_call_to_json(call=self.api_root["settings"])

    def get_display_list(self) -> List:
        """Gets a list of all `display` instances.

        Returns:
            List: A list containing dicts of all `display` instances.
        """
        return self._api_call_to_json(call=self.api_root["display"])

    def get_vessel_list(self) -> List:
        """Gets a list of all `vessel` instances.

        Returns:
            List: A list containing dicts of all `vessel` instances.
        """
        return self._api_call_to_json(call=self.api_root["vessel"])

    def get_marker_list(self) -> List:
        """Gets a list of all `marker` instances.

        Returns:
            List: A list containing dicts of all `marker` instances.
        """
        return self._api_call_to_json(call=self.api_root["marker"])

    def get_cruise_list(self) -> List:
        """Gets a list of all `cruise` instances.

        Returns:
            List: A list containing dicts of all `cruise` instances.
        """
        return self._api_call_to_json(call=self.api_root["cruise"])

    def get_participants_list(self) -> List:
        """Gets a list of all `participants` instances.

        Returns:
            List: A list containing dicts of all `participants` instances.
        """
        return self._api_call_to_json(call=self.api_root["participants"])

    def get_events_list(self) -> List:
        """Gets a list of all `events` instances.

        Returns:
            List: A list containing dicts of all `events` instances.
        """
        return self._api_call_to_json(call=self.api_root["events"])

    def get_subevent_list(self) -> List:
        """Gets a list of all `subevent` instances.

        Returns:
            List: A list containing dicts of all `subevent` instances.
        """
        return self._api_call_to_json(call=self.api_root["subevent"])

    def get_asset_list(self) -> List:
        """Gets a list of all `asset` instances.

        Returns:
            List: A list containing dicts of all `asset` instances.
        """
        return self._api_call_to_json(call=self.api_root["asset"])

    def get_station_list(self) -> List:
        """Gets a list of all `station` instances.

        Returns:
            List: A list containing dicts of all `station` instances.
        """
        return self._api_call_to_json(call=self.api_root["station"])

    def get_port_list(self) -> List:
        """Gets a list of all `port` instances.

        Returns:
            List: A list containing dicts of all `port` instances.
        """
        return self._api_call_to_json(call=self.api_root["port"])

    def get_icon_list(self) -> List:
        """Gets a list of all `icon` instances.

        Returns:
            List: A list containing dicts of all `icon` instances.
        """
        return self._api_call_to_json(call=self.api_root["icon"])

    def get_routes_list(self) -> List:
        """Gets a list of all `routes` instances.

        Returns:
            List: A list containing dicts of all `routes` instances.
        """
        return self._api_call_to_json(call=self.api_root["routes"])

    def get_chart_metadata_list(self) -> List:
        """Gets a list of all `chart_metadata` instances.

        Returns:
            List: A list containing dicts of all `chart_metadata` instances.
        """
        return self._api_call_to_json(call=self.api_root["chart_metadata"])

    def get_sensor_list(self) -> List:
        """Gets a list of all `sensor` instances.

        Returns:
            List: A list containing dicts of all `sensor` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor"])

    def get_sensor_group_list(self) -> List:
        """Gets a list of all `sensor_group` instances.

        Returns:
            List: A list containing dicts of all `sensor_group` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_group"])

    def get_parameter_list(self) -> List:
        """Gets a list of all `parameter` instances.

        Returns:
            List: A list containing dicts of all `parameter` instances.
        """
        return self._api_call_to_json(call=self.api_root["parameter"])

    def get_flags_list(self) -> List:
        """Gets a list of all `flags` instances.

        Returns:
            List: A list containing dicts of all `flags` instances.
        """
        return self._api_call_to_json(call=self.api_root["flags"])

    def get_sensor_log_list(self) -> List:
        """Gets a list of all `sensor_log` instances.

        Returns:
            List: A list containing dicts of all `sensor_log` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_log"])

    def get_sensor_coeffs_list(self) -> List:
        """Gets a list of all `sensor_coeffs` instances.

        Returns:
            List: A list containing dicts of all `sensor_coeffs` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_coeffs"])

    def get_sensor_midcal_list(self) -> List:
        """Gets a list of all `sensor_midcal` instances.

        Returns:
            List: A list containing dicts of all `sensor_midcal` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_midcal"])

    def get_document_list(self) -> List:
        """Gets a list of all `document` instances.

        Returns:
            List: A list containing dicts of all `document` instances.
        """
        return self._api_call_to_json(call=self.api_root["document"])

    def get_vendor_list(self) -> List:
        """Gets a list of all `vendor` instances.

        Returns:
            List: A list containing dicts of all `vendor` instances.
        """
        return self._api_call_to_json(call=self.api_root["vendor"])

    def get_data_sources_list(self) -> List:
        """Gets a list of all `data_sources` instances.

        Returns:
            List: A list containing dicts of all `data_sources` instances.
        """
        return self._api_call_to_json(call=self.api_root["data_sources"])

    def get_valve_list(self) -> List:
        """Gets a list of all `valve` instances.

        Returns:
            List: A list containing dicts of all `valve` instances.
        """
        return self._api_call_to_json(call=self.api_root["valve"])

    def get_last_obs_list(self) -> List:
        """Gets a list of all `last_obs` instances.

        Returns:
            List: A list containing dicts of all `last_obs` instances.
        """
        return self._api_call_to_json(call=self.api_root["last_obs"])

    def get_gnss_gga_bow_list(self) -> List:
        """Gets a list of all `gnss_gga_bow` instances.

        Returns:
            List: A list containing dicts of all `gnss_gga_bow` instances.
        """
        return self._api_call_to_json(call=self.api_root["gnss_gga_bow"])

    def get_gnss_vtg_bow_list(self) -> List:
        """Gets a list of all `gnss_vtg_bow` instances.

        Returns:
            List: A list containing dicts of all `gnss_vtg_bow` instances.
        """
        return self._api_call_to_json(call=self.api_root["gnss_vtg_bow"])

    def get_gnss_gsv_bow_list(self) -> List:
        """Gets a list of all `gnss_gsv_bow` instances.

        Returns:
            List: A list containing dicts of all `gnss_gsv_bow` instances.
        """
        return self._api_call_to_json(call=self.api_root["gnss_gsv_bow"])

    def get_gyro_brdg_list(self) -> List:
        """Gets a list of all `gyro_brdg` instances.

        Returns:
            List: A list containing dicts of all `gyro_brdg` instances.
        """
        return self._api_call_to_json(call=self.api_root["gyro_brdg"])

    def get_anemo_mmast_list(self) -> List:
        """Gets a list of all `anemo_mmast` instances.

        Returns:
            List: A list containing dicts of all `anemo_mmast` instances.
        """
        return self._api_call_to_json(call=self.api_root["anemo_mmast"])

    def get_metstn_stbd_list(self) -> List:
        """Gets a list of all `metstn_stbd` instances.

        Returns:
            List: A list containing dicts of all `metstn_stbd` instances.
        """
        return self._api_call_to_json(call=self.api_root["metstn_stbd"])

    def get_metstn_bow_list(self) -> List:
        """Gets a list of all `metstn_bow` instances.

        Returns:
            List: A list containing dicts of all `metstn_bow` instances.
        """
        return self._api_call_to_json(call=self.api_root["metstn_bow"])

    def get_mru_list(self) -> List:
        """Gets a list of all `mru` instances.

        Returns:
            List: A list containing dicts of all `mru` instances.
        """
        return self._api_call_to_json(call=self.api_root["mru"])

    def get_tsg_flth_list(self) -> List:
        """Gets a list of all `tsg_flth` instances.

        Returns:
            List: A list containing dicts of all `tsg_flth` instances.
        """
        return self._api_call_to_json(call=self.api_root["tsg_flth"])

    def get_transmiss_flth_list(self) -> List:
        """Gets a list of all `transmiss_flth` instances.

        Returns:
            List: A list containing dicts of all `transmiss_flth` instances.
        """
        return self._api_call_to_json(call=self.api_root["transmiss_flth"])

    def get_therm_hull_list(self) -> List:
        """Gets a list of all `therm_hull` instances.

        Returns:
            List: A list containing dicts of all `therm_hull` instances.
        """
        return self._api_call_to_json(call=self.api_root["therm_hull"])

    def get_echo_well_list(self) -> List:
        """Gets a list of all `echo_well` instances.

        Returns:
            List: A list containing dicts of all `echo_well` instances.
        """
        return self._api_call_to_json(call=self.api_root["echo_well"])

    def get_fluor_flth_list(self) -> List:
        """Gets a list of all `fluor_flth` instances.

        Returns:
            List: A list containing dicts of all `fluor_flth` instances.
        """
        return self._api_call_to_json(call=self.api_root["fluor_flth"])

    def get_therm_fwd_list(self) -> List:
        """Gets a list of all `therm_fwd` instances.

        Returns:
            List: A list containing dicts of all `therm_fwd` instances.
        """
        return self._api_call_to_json(call=self.api_root["therm_fwd"])

    def get_par_mmast_list(self) -> List:
        """Gets a list of all `par_mmast` instances.

        Returns:
            List: A list containing dicts of all `par_mmast` instances.
        """
        return self._api_call_to_json(call=self.api_root["par_mmast"])

    def get_rad_mmast_list(self) -> List:
        """Gets a list of all `rad_mmast` instances.

        Returns:
            List: A list containing dicts of all `rad_mmast` instances.
        """
        return self._api_call_to_json(call=self.api_root["rad_mmast"])

    def get_rain_mmast_list(self) -> List:
        """Gets a list of all `rain_mmast` instances.

        Returns:
            List: A list containing dicts of all `rain_mmast` instances.
        """
        return self._api_call_to_json(call=self.api_root["rain_mmast"])

    def get_speedlog_well_list(self) -> List:
        """Gets a list of all `speedlog_well` instances.

        Returns:
            List: A list containing dicts of all `speedlog_well` instances.
        """
        return self._api_call_to_json(call=self.api_root["speedlog_well"])

    def get_true_winds_list(self) -> List:
        """Gets a list of all `true_winds` instances.

        Returns:
            List: A list containing dicts of all `true_winds` instances.
        """
        return self._api_call_to_json(call=self.api_root["true_winds"])

    def get_sensor_float_1_list(self) -> List:
        """Gets a list of all `sensor_float_1` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_1` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_1"])

    def get_sensor_float_2_list(self) -> List:
        """Gets a list of all `sensor_float_2` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_2` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_2"])

    def get_sensor_float_3_list(self) -> List:
        """Gets a list of all `sensor_float_3` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_3` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_3"])

    def get_sensor_float_4_list(self) -> List:
        """Gets a list of all `sensor_float_4` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_4` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_4"])

    def get_sensor_float_5_list(self) -> List:
        """Gets a list of all `sensor_float_5` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_5` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_5"])

    def get_sensor_float_6_list(self) -> List:
        """Gets a list of all `sensor_float_6` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_6` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_6"])

    def get_sensor_float_7_list(self) -> List:
        """Gets a list of all `sensor_float_7` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_7` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_7"])

    def get_sensor_float_8_list(self) -> List:
        """Gets a list of all `sensor_float_8` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_8` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_8"])

    def get_sensor_float_9_list(self) -> List:
        """Gets a list of all `sensor_float_9` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_9` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_9"])

    def get_sensor_float_10_list(self) -> List:
        """Gets a list of all `sensor_float_10` instances.

        Returns:
            List: A list containing dicts of all `sensor_float_10` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_float_10"])

    def get_sensor_integer_1_list(self) -> List:
        """Gets a list of all `sensor_integer_1` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_1` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_1"])

    def get_sensor_integer_2_list(self) -> List:
        """Gets a list of all `sensor_integer_2` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_2` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_2"])

    def get_sensor_integer_3_list(self) -> List:
        """Gets a list of all `sensor_integer_3` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_3` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_3"])

    def get_sensor_integer_4_list(self) -> List:
        """Gets a list of all `sensor_integer_4` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_4` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_4"])

    def get_sensor_integer_5_list(self) -> List:
        """Gets a list of all `sensor_integer_5` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_5` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_5"])

    def get_sensor_integer_6_list(self) -> List:
        """Gets a list of all `sensor_integer_6` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_6` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_6"])

    def get_sensor_integer_7_list(self) -> List:
        """Gets a list of all `sensor_integer_7` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_7` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_7"])

    def get_sensor_integer_8_list(self) -> List:
        """Gets a list of all `sensor_integer_8` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_8` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_8"])

    def get_sensor_integer_9_list(self) -> List:
        """Gets a list of all `sensor_integer_9` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_9` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_9"])

    def get_sensor_integer_10_list(self) -> List:
        """Gets a list of all `sensor_integer_10` instances.

        Returns:
            List: A list containing dicts of all `sensor_integer_10` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_integer_10"])

    def get_sensor_point_1_list(self) -> List:
        """Gets a list of all `sensor_point_1` instances.

        Returns:
            List: A list containing dicts of all `sensor_point_1` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_point_1"])

    def get_sensor_point_2_list(self) -> List:
        """Gets a list of all `sensor_point_2` instances.

        Returns:
            List: A list containing dicts of all `sensor_point_2` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_point_2"])

    def get_sensor_text_1_list(self) -> List:
        """Gets a list of all `sensor_text_1` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_1` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_1"])

    def get_sensor_text_2_list(self) -> List:
        """Gets a list of all `sensor_text_2` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_2` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_2"])

    def get_sensor_text_3_list(self) -> List:
        """Gets a list of all `sensor_text_3` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_3` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_3"])

    def get_sensor_text_4_list(self) -> List:
        """Gets a list of all `sensor_text_4` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_4` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_4"])

    def get_sensor_text_5_list(self) -> List:
        """Gets a list of all `sensor_text_5` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_5` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_5"])

    def get_sensor_text_6_list(self) -> List:
        """Gets a list of all `sensor_text_6` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_6` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_6"])

    def get_sensor_text_7_list(self) -> List:
        """Gets a list of all `sensor_text_7` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_7` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_7"])

    def get_sensor_text_8_list(self) -> List:
        """Gets a list of all `sensor_text_8` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_8` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_8"])

    def get_sensor_text_9_list(self) -> List:
        """Gets a list of all `sensor_text_9` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_9` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_9"])

    def get_sensor_text_10_list(self) -> List:
        """Gets a list of all `sensor_text_10` instances.

        Returns:
            List: A list containing dicts of all `sensor_text_10` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_text_10"])

    def get_sensor_mixed_1_list(self) -> List:
        """Gets a list of all `sensor_mixed_1` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_1` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_1"])

    def get_sensor_mixed_2_list(self) -> List:
        """Gets a list of all `sensor_mixed_2` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_2` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_2"])

    def get_sensor_mixed_3_list(self) -> List:
        """Gets a list of all `sensor_mixed_3` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_3` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_3"])

    def get_sensor_mixed_4_list(self) -> List:
        """Gets a list of all `sensor_mixed_4` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_4` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_4"])

    def get_sensor_mixed_5_list(self) -> List:
        """Gets a list of all `sensor_mixed_5` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_5` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_5"])

    def get_sensor_mixed_6_list(self) -> List:
        """Gets a list of all `sensor_mixed_6` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_6` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_6"])

    def get_sensor_mixed_7_list(self) -> List:
        """Gets a list of all `sensor_mixed_7` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_7` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_7"])

    def get_sensor_mixed_8_list(self) -> List:
        """Gets a list of all `sensor_mixed_8` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_8` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_8"])

    def get_sensor_mixed_9_list(self) -> List:
        """Gets a list of all `sensor_mixed_9` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_9` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_9"])

    def get_sensor_mixed_10_list(self) -> List:
        """Gets a list of all `sensor_mixed_10` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_10` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_10"])

    def get_sensor_mixed_11_list(self) -> List:
        """Gets a list of all `sensor_mixed_11` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_11` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_11"])

    def get_sensor_mixed_12_list(self) -> List:
        """Gets a list of all `sensor_mixed_12` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_12` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_12"])

    def get_sensor_mixed_13_list(self) -> List:
        """Gets a list of all `sensor_mixed_13` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_13` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_13"])

    def get_sensor_mixed_14_list(self) -> List:
        """Gets a list of all `sensor_mixed_14` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_14` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_14"])

    def get_sensor_mixed_15_list(self) -> List:
        """Gets a list of all `sensor_mixed_15` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_15` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_15"])

    def get_sensor_mixed_16_list(self) -> List:
        """Gets a list of all `sensor_mixed_16` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_16` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_16"])

    def get_sensor_mixed_17_list(self) -> List:
        """Gets a list of all `sensor_mixed_17` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_17` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_17"])

    def get_sensor_mixed_18_list(self) -> List:
        """Gets a list of all `sensor_mixed_18` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_18` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_18"])

    def get_sensor_mixed_19_list(self) -> List:
        """Gets a list of all `sensor_mixed_19` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_19` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_19"])

    def get_sensor_mixed_20_list(self) -> List:
        """Gets a list of all `sensor_mixed_20` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixed_20` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixed_20"])

    def get_sensor_mixlg_1_list(self) -> List:
        """Gets a list of all `sensor_mixlg_1` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_1` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_1"])

    def get_sensor_mixlg_2_list(self) -> List:
        """Gets a list of all `sensor_mixlg_2` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_2` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_2"])

    def get_sensor_mixlg_3_list(self) -> List:
        """Gets a list of all `sensor_mixlg_3` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_3` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_3"])

    def get_sensor_mixlg_4_list(self) -> List:
        """Gets a list of all `sensor_mixlg_4` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_4` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_4"])

    def get_sensor_mixlg_5_list(self) -> List:
        """Gets a list of all `sensor_mixlg_5` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_5` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_5"])

    def get_sensor_mixlg_6_list(self) -> List:
        """Gets a list of all `sensor_mixlg_6` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_6` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_6"])

    def get_sensor_mixlg_7_list(self) -> List:
        """Gets a list of all `sensor_mixlg_7` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_7` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_7"])

    def get_sensor_mixlg_8_list(self) -> List:
        """Gets a list of all `sensor_mixlg_8` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_8` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_8"])

    def get_sensor_mixlg_9_list(self) -> List:
        """Gets a list of all `sensor_mixlg_9` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_9` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_9"])

    def get_sensor_mixlg_10_list(self) -> List:
        """Gets a list of all `sensor_mixlg_10` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_10` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_10"])

    def get_sensor_mixlg_11_list(self) -> List:
        """Gets a list of all `sensor_mixlg_11` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_11` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_11"])

    def get_sensor_mixlg_12_list(self) -> List:
        """Gets a list of all `sensor_mixlg_12` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_12` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_12"])

    def get_sensor_mixlg_13_list(self) -> List:
        """Gets a list of all `sensor_mixlg_13` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_13` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_13"])

    def get_sensor_mixlg_14_list(self) -> List:
        """Gets a list of all `sensor_mixlg_14` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_14` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_14"])

    def get_sensor_mixlg_15_list(self) -> List:
        """Gets a list of all `sensor_mixlg_15` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_15` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_15"])

    def get_sensor_mixlg_16_list(self) -> List:
        """Gets a list of all `sensor_mixlg_16` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_16` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_16"])

    def get_sensor_mixlg_17_list(self) -> List:
        """Gets a list of all `sensor_mixlg_17` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_17` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_17"])

    def get_sensor_mixlg_18_list(self) -> List:
        """Gets a list of all `sensor_mixlg_18` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_18` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_18"])

    def get_sensor_mixlg_19_list(self) -> List:
        """Gets a list of all `sensor_mixlg_19` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_19` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_19"])

    def get_sensor_mixlg_20_list(self) -> List:
        """Gets a list of all `sensor_mixlg_20` instances.

        Returns:
            List: A list containing dicts of all `sensor_mixlg_20` instances.
        """
        return self._api_call_to_json(call=self.api_root["sensor_mixlg_20"])

    def get_custom_sensor_1_list(self) -> List:
        """Gets a list of all `custom_sensor_1` instances.

        Returns:
            List: A list containing dicts of all `custom_sensor_1` instances.
        """
        return self._api_call_to_json(call=self.api_root["custom_sensor_1"])

    def get_custom_sensor_2_list(self) -> List:
        """Gets a list of all `custom_sensor_2` instances.

        Returns:
            List: A list containing dicts of all `custom_sensor_2` instances.
        """
        return self._api_call_to_json(call=self.api_root["custom_sensor_2"])

    def get_custom_sensor_3_list(self) -> List:
        """Gets a list of all `custom_sensor_3` instances.

        Returns:
            List: A list containing dicts of all `custom_sensor_3` instances.
        """
        return self._api_call_to_json(call=self.api_root["custom_sensor_3"])

    def get_binned_default_flow_list(self) -> List:
        """Gets a list of all `binned_default_flow` instances.

        Returns:
            List: A list containing dicts of all `binned_default_flow`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_default_flow"]
        )

    def get_binned_default_flow_map_list(self) -> List:
        """Gets a list of all `binned_default_flow_map` instances.

        Returns:
            List: A list containing dicts of all `binned_default_flow_map`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_default_flow_map"]
        )

    def get_binned_default_flow_map2_list(self) -> List:
        """Gets a list of all `binned_default_flow_map2` instances.

        Returns:
            List: A list containing dicts of all `binned_default_flow_map2`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_default_flow_map2"]
        )

    def get_binned_custom_flow_list(self) -> List:
        """Gets a list of all `binned_custom_flow` instances.

        Returns:
            List: A list containing dicts of all `binned_custom_flow`
                instances.
        """
        return self._api_call_to_json(call=self.api_root["binned_custom_flow"])

    def get_binned_default_met_list(self) -> List:
        """Gets a list of all `binned_default_met` instances.

        Returns:
            List: A list containing dicts of all `binned_default_met`
                instances.
        """
        return self._api_call_to_json(call=self.api_root["binned_default_met"])

    def get_binned_default_met_map_list(self) -> List:
        """Gets a list of all `binned_default_met_map` instances.

        Returns:
            List: A list containing dicts of all `binned_default_met_map`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_default_met_map"]
        )

    def get_binned_custom_met_list(self) -> List:
        """Gets a list of all `binned_custom_met` instances.

        Returns:
            List: A list containing dicts of all `binned_custom_met` instances.
        """
        return self._api_call_to_json(call=self.api_root["binned_custom_met"])

    def get_binned_default_nav_list(self) -> List:
        """Gets a list of all `binned_default_nav` instances.

        Returns:
            List: A list containing dicts of all `binned_default_nav`
                instances.
        """
        return self._api_call_to_json(call=self.api_root["binned_default_nav"])

    def get_binned_default_nav_map_list(self) -> List:
        """Gets a list of all `binned_default_nav_map` instances.

        Returns:
            List: A list containing dicts of all `binned_default_nav_map`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_default_nav_map"]
        )

    def get_binned_nav_1min_list(self) -> List:
        """Gets a list of all `binned_nav_1min` instances.

        Returns:
            List: A list containing dicts of all `binned_nav_1min` instances.
        """
        return self._api_call_to_json(call=self.api_root["binned_nav_1min"])

    def get_binned_1min_list(self) -> List:
        """Gets a list of all `binned_1min` instances.

        Returns:
            List: A list containing dicts of all `binned_1min` instances.
        """
        return self._api_call_to_json(call=self.api_root["binned_1min"])

    def get_binned_xmin_list(self) -> List:
        """Gets a list of all `binned_xmin` instances.

        Returns:
            List: A list containing dicts of all `binned_xmin` instances.
        """
        return self._api_call_to_json(call=self.api_root["binned_xmin"])

    def get_binned_custom_nav_list(self) -> List:
        """Gets a list of all `binned_custom_nav` instances.

        Returns:
            List: A list containing dicts of all `binned_custom_nav` instances.
        """
        return self._api_call_to_json(call=self.api_root["binned_custom_nav"])

    def get_binned_default_wind_list(self) -> List:
        """Gets a list of all `binned_default_wind` instances.

        Returns:
            List: A list containing dicts of all `binned_default_wind`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_default_wind"]
        )

    def get_binned_default_wind_map_list(self) -> List:
        """Gets a list of all `binned_default_wind_map` instances.

        Returns:
            List: A list containing dicts of all `binned_default_wind_map`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_default_wind_map"]
        )

    def get_binned_custom_wind_list(self) -> List:
        """Gets a list of all `binned_custom_wind` instances.

        Returns:
            List: A list containing dicts of all `binned_custom_wind`
                instances.
        """
        return self._api_call_to_json(call=self.api_root["binned_custom_wind"])

    def get_binned_default_other_list(self) -> List:
        """Gets a list of all `binned_default_other` instances.

        Returns:
            List: A list containing dicts of all `binned_default_other`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_default_other"]
        )

    def get_binned_default_other_map_list(self) -> List:
        """Gets a list of all `binned_default_other_map` instances.

        Returns:
            List: A list containing dicts of all `binned_default_other_map`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_default_other_map"]
        )

    def get_binned_custom_other_list(self) -> List:
        """Gets a list of all `binned_custom_other` instances.

        Returns:
            List: A list containing dicts of all `binned_custom_other`
                instances.
        """
        return self._api_call_to_json(
            call=self.api_root["binned_custom_other"]
        )

    def get_alert_config_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `alert_config`.

        Args:
            instance (str, optional): The string to search for within
                `alert_config`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `alert_config` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="alert_config"
        )
        return self._api_call_to_json(call=call)

    def get_alert_action_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `alert_action`.

        Args:
            instance (str, optional): The string to search for within
                `alert_action`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `alert_action` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="alert_action"
        )
        return self._api_call_to_json(call=call)

    def get_alert_current_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `alert_current`.

        Args:
            instance (str, optional): The string to search for within
                `alert_current`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `alert_current` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="alert_current"
        )
        return self._api_call_to_json(call=call)

    def get_alert_archive_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `alert_archive`.

        Args:
            instance (str, optional): The string to search for within
                `alert_archive`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `alert_archive` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="alert_archive"
        )
        return self._api_call_to_json(call=call)

    def get_channel_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `channel`.

        Args:
            instance (str, optional): The string to search for within
                `channel`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `channel`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="channel")
        return self._api_call_to_json(call=call)

    def get_settings_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `settings`.

        Args:
            instance (str, optional): The string to search for within
                `settings`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `settings`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="settings")
        return self._api_call_to_json(call=call)

    def get_display_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `display`.

        Args:
            instance (str, optional): The string to search for within
                `display`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `display`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="display")
        return self._api_call_to_json(call=call)

    def get_vessel_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `vessel`.

        Args:
            instance (str, optional): The string to search for within `vessel`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `vessel`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="vessel")
        return self._api_call_to_json(call=call)

    def get_marker_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `marker`.

        Args:
            instance (str, optional): The string to search for within `marker`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `marker`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="marker")
        return self._api_call_to_json(call=call)

    def get_cruise_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `cruise`.

        Args:
            instance (str, optional): The string to search for within `cruise`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `cruise`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="cruise")
        return self._api_call_to_json(call=call)

    def get_participants_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `participants`.

        Args:
            instance (str, optional): The string to search for within
                `participants`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `participants` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="participants"
        )
        return self._api_call_to_json(call=call)

    def get_events_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `events`.

        Args:
            instance (str, optional): The string to search for within `events`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `events`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="events")
        return self._api_call_to_json(call=call)

    def get_subevent_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `subevent`.

        Args:
            instance (str, optional): The string to search for within
                `subevent`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `subevent` instance.
        """

        call = self._format_instance_call(instance=instance, call="subevent")
        return self._api_call_to_json(call=call)

    def get_asset_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `asset`.

        Args:
            instance (str, optional): The string to search for within `asset`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `asset`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="asset")
        return self._api_call_to_json(call=call)

    def get_station_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `station`.

        Args:
            instance (str, optional): The string to search for within
                `station`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `station`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="station")
        return self._api_call_to_json(call=call)

    def get_port_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `port`.

        Args:
            instance (str, optional): The string to search for within `port`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `port`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="port")
        return self._api_call_to_json(call=call)

    def get_icon_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `icon`.

        Args:
            instance (str, optional): The string to search for within `icon`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `icon`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="icon")
        return self._api_call_to_json(call=call)

    def get_routes_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `routes`.

        Args:
            instance (str, optional): The string to search for within `routes`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `routes`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="routes")
        return self._api_call_to_json(call=call)

    def get_chart_metadata_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `chart_metadata`.

        Args:
            instance (str, optional): The string to search for within
                `chart_metadata`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `chart_metadata` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="chart_metadata"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor`.

        Args:
            instance (str, optional): The string to search for within `sensor`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `sensor`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="sensor")
        return self._api_call_to_json(call=call)

    def get_sensor_group_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_group`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_group`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_group` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_group"
        )
        return self._api_call_to_json(call=call)

    def get_parameter_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `parameter`.

        Args:
            instance (str, optional): The string to search for within
                `parameter`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `parameter`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="parameter")
        return self._api_call_to_json(call=call)

    def get_flags_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `flags`.

        Args:
            instance (str, optional): The string to search for within `flags`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `flags`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="flags")
        return self._api_call_to_json(call=call)

    def get_sensor_log_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_log`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_log`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_log` instance.
        """

        call = self._format_instance_call(instance=instance, call="sensor_log")
        return self._api_call_to_json(call=call)

    def get_sensor_coeffs_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_coeffs`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_coeffs`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_coeffs` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_coeffs"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_midcal_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_midcal`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_midcal`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_midcal` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_midcal"
        )
        return self._api_call_to_json(call=call)

    def get_document_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `document`.

        Args:
            instance (str, optional): The string to search for within
                `document`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `document`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="document")
        return self._api_call_to_json(call=call)

    def get_vendor_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `vendor`.

        Args:
            instance (str, optional): The string to search for within `vendor`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `vendor`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="vendor")
        return self._api_call_to_json(call=call)

    def get_data_sources_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `data_sources`.

        Args:
            instance (str, optional): The string to search for within
                `data_sources`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `data_sources` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="data_sources"
        )
        return self._api_call_to_json(call=call)

    def get_valve_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `valve`.

        Args:
            instance (str, optional): The string to search for within `valve`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `valve`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="valve")
        return self._api_call_to_json(call=call)

    def get_last_obs_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `last_obs`.

        Args:
            instance (str, optional): The string to search for within
                `last_obs`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `last_obs`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="last_obs")
        return self._api_call_to_json(call=call)

    def get_gnss_gga_bow_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `gnss_gga_bow`.

        Args:
            instance (str, optional): The string to search for within
                `gnss_gga_bow`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `gnss_gga_bow` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="gnss_gga_bow"
        )
        return self._api_call_to_json(call=call)

    def get_gnss_vtg_bow_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `gnss_vtg_bow`.

        Args:
            instance (str, optional): The string to search for within
                `gnss_vtg_bow`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `gnss_vtg_bow` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="gnss_vtg_bow"
        )
        return self._api_call_to_json(call=call)

    def get_gnss_gsv_bow_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `gnss_gsv_bow`.

        Args:
            instance (str, optional): The string to search for within
                `gnss_gsv_bow`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `gnss_gsv_bow` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="gnss_gsv_bow"
        )
        return self._api_call_to_json(call=call)

    def get_gyro_brdg_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `gyro_brdg`.

        Args:
            instance (str, optional): The string to search for within
                `gyro_brdg`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `gyro_brdg`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="gyro_brdg")
        return self._api_call_to_json(call=call)

    def get_anemo_mmast_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `anemo_mmast`.

        Args:
            instance (str, optional): The string to search for within
                `anemo_mmast`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `anemo_mmast` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="anemo_mmast"
        )
        return self._api_call_to_json(call=call)

    def get_metstn_stbd_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `metstn_stbd`.

        Args:
            instance (str, optional): The string to search for within
                `metstn_stbd`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `metstn_stbd` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="metstn_stbd"
        )
        return self._api_call_to_json(call=call)

    def get_metstn_bow_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `metstn_bow`.

        Args:
            instance (str, optional): The string to search for within
                `metstn_bow`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `metstn_bow` instance.
        """

        call = self._format_instance_call(instance=instance, call="metstn_bow")
        return self._api_call_to_json(call=call)

    def get_mru_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `mru`.

        Args:
            instance (str, optional): The string to search for within `mru`.
                Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `mru`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="mru")
        return self._api_call_to_json(call=call)

    def get_tsg_flth_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `tsg_flth`.

        Args:
            instance (str, optional): The string to search for within
                `tsg_flth`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `tsg_flth`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="tsg_flth")
        return self._api_call_to_json(call=call)

    def get_transmiss_flth_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `transmiss_flth`.

        Args:
            instance (str, optional): The string to search for within
                `transmiss_flth`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `transmiss_flth` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="transmiss_flth"
        )
        return self._api_call_to_json(call=call)

    def get_therm_hull_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `therm_hull`.

        Args:
            instance (str, optional): The string to search for within
                `therm_hull`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `therm_hull` instance.
        """

        call = self._format_instance_call(instance=instance, call="therm_hull")
        return self._api_call_to_json(call=call)

    def get_echo_well_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `echo_well`.

        Args:
            instance (str, optional): The string to search for within
                `echo_well`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `echo_well`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="echo_well")
        return self._api_call_to_json(call=call)

    def get_fluor_flth_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `fluor_flth`.

        Args:
            instance (str, optional): The string to search for within
                `fluor_flth`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `fluor_flth` instance.
        """

        call = self._format_instance_call(instance=instance, call="fluor_flth")
        return self._api_call_to_json(call=call)

    def get_therm_fwd_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `therm_fwd`.

        Args:
            instance (str, optional): The string to search for within
                `therm_fwd`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `therm_fwd`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="therm_fwd")
        return self._api_call_to_json(call=call)

    def get_par_mmast_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `par_mmast`.

        Args:
            instance (str, optional): The string to search for within
                `par_mmast`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `par_mmast`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="par_mmast")
        return self._api_call_to_json(call=call)

    def get_rad_mmast_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `rad_mmast`.

        Args:
            instance (str, optional): The string to search for within
                `rad_mmast`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the `rad_mmast`
                instance.
        """

        call = self._format_instance_call(instance=instance, call="rad_mmast")
        return self._api_call_to_json(call=call)

    def get_rain_mmast_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `rain_mmast`.

        Args:
            instance (str, optional): The string to search for within
                `rain_mmast`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `rain_mmast` instance.
        """

        call = self._format_instance_call(instance=instance, call="rain_mmast")
        return self._api_call_to_json(call=call)

    def get_speedlog_well_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `speedlog_well`.

        Args:
            instance (str, optional): The string to search for within
                `speedlog_well`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `speedlog_well` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="speedlog_well"
        )
        return self._api_call_to_json(call=call)

    def get_true_winds_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `true_winds`.

        Args:
            instance (str, optional): The string to search for within
                `true_winds`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `true_winds` instance.
        """

        call = self._format_instance_call(instance=instance, call="true_winds")
        return self._api_call_to_json(call=call)

    def get_sensor_float_1_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_1`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_1`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_1` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_1"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_float_2_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_2`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_2`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_2` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_2"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_float_3_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_3`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_3`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_3` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_3"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_float_4_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_4`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_4`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_4` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_4"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_float_5_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_5`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_5`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_5` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_5"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_float_6_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_6`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_6`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_6` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_6"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_float_7_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_7`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_7`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_7` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_7"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_float_8_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_8`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_8`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_8` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_8"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_float_9_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_9`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_9`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_9` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_9"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_float_10_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_float_10`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_float_10`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_float_10` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_float_10"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_1_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_1`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_1`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_1` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_1"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_2_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_2`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_2`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_2` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_2"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_3_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_3`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_3`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_3` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_3"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_4_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_4`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_4`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_4` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_4"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_5_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_5`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_5`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_5` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_5"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_6_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_6`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_6`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_6` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_6"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_7_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_7`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_7`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_7` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_7"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_8_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_8`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_8`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_8` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_8"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_9_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_9`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_9`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_9` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_9"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_integer_10_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_integer_10`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_integer_10`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_integer_10` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_integer_10"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_point_1_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_point_1`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_point_1`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_point_1` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_point_1"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_point_2_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_point_2`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_point_2`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_point_2` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_point_2"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_1_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_1`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_1`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_1` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_1"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_2_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_2`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_2`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_2` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_2"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_3_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_3`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_3`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_3` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_3"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_4_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_4`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_4`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_4` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_4"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_5_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_5`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_5`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_5` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_5"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_6_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_6`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_6`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_6` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_6"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_7_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_7`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_7`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_7` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_7"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_8_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_8`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_8`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_8` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_8"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_9_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_9`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_9`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_9` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_9"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_text_10_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_text_10`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_text_10`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_text_10` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_text_10"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_1_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_1`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_1`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_1` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_1"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_2_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_2`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_2`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_2` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_2"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_3_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_3`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_3`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_3` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_3"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_4_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_4`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_4`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_4` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_4"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_5_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_5`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_5`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_5` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_5"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_6_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_6`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_6`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_6` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_6"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_7_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_7`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_7`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_7` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_7"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_8_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_8`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_8`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_8` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_8"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_9_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_9`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_9`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_9` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_9"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_10_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_10`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_10`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_10` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_10"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_11_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_11`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_11`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_11` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_11"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_12_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_12`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_12`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_12` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_12"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_13_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_13`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_13`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_13` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_13"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_14_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_14`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_14`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_14` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_14"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_15_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_15`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_15`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_15` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_15"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_16_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_16`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_16`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_16` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_16"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_17_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_17`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_17`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_17` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_17"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_18_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_18`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_18`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_18` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_18"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_19_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_19`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_19`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_19` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_19"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixed_20_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixed_20`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixed_20`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixed_20` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixed_20"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_1_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_1`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_1`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_1` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_1"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_2_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_2`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_2`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_2` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_2"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_3_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_3`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_3`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_3` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_3"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_4_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_4`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_4`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_4` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_4"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_5_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_5`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_5`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_5` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_5"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_6_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_6`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_6`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_6` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_6"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_7_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_7`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_7`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_7` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_7"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_8_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_8`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_8`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_8` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_8"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_9_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_9`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_9`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_9` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_9"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_10_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_10`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_10`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_10` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_10"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_11_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_11`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_11`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_11` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_11"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_12_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_12`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_12`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_12` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_12"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_13_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_13`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_13`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_13` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_13"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_14_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_14`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_14`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_14` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_14"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_15_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_15`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_15`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_15` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_15"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_16_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_16`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_16`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_16` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_16"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_17_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_17`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_17`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_17` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_17"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_18_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_18`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_18`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_18` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_18"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_19_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_19`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_19`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_19`instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_19"
        )
        return self._api_call_to_json(call=call)

    def get_sensor_mixlg_20_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `sensor_mixlg_20`.

        Args:
            instance (str, optional): The string to search for within
                `sensor_mixlg_20`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `sensor_mixlg_20` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="sensor_mixlg_20"
        )
        return self._api_call_to_json(call=call)

    def get_custom_sensor_1_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `custom_sensor_1`.

        Args:
            instance (str, optional): The string to search for within
                `custom_sensor_1`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `custom_sensor_1` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="custom_sensor_1"
        )
        return self._api_call_to_json(call=call)

    def get_custom_sensor_2_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `custom_sensor_2`.

        Args:
            instance (str, optional): The string to search for within
                `custom_sensor_2`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `custom_sensor_2` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="custom_sensor_2"
        )
        return self._api_call_to_json(call=call)

    def get_custom_sensor_3_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `custom_sensor_3`.

        Args:
            instance (str, optional): The string to search for within
                `custom_sensor_3`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `custom_sensor_3` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="custom_sensor_3"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_flow_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_default_flow`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_flow`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_flow` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_flow"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_flow_map_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_default_flow_map`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_flow_map`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_flow_map` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_flow_map"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_flow_map2_instance(
        self, instance: str = ""
    ) -> dict:
        """Gets a single instance of `binned_default_flow_map2`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_flow_map2`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_flow_map2` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_flow_map2"
        )
        return self._api_call_to_json(call=call)

    def get_binned_custom_flow_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_custom_flow`.

        Args:
            instance (str, optional): The string to search for within
                `binned_custom_flow`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_custom_flow` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_custom_flow"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_met_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_default_met`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_met`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_met` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_met"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_met_map_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_default_met_map`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_met_map`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_met_map` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_met_map"
        )
        return self._api_call_to_json(call=call)

    def get_binned_custom_met_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_custom_met`.

        Args:
            instance (str, optional): The string to search for within
                `binned_custom_met`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_custom_met` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_custom_met"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_nav_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_default_nav`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_nav`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_nav` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_nav"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_nav_map_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_default_nav_map`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_nav_map`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_nav_map` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_nav_map"
        )
        return self._api_call_to_json(call=call)

    def get_binned_nav_1min_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_nav_1min`.

        Args:
            instance (str, optional): The string to search for within
                `binned_nav_1min`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_nav_1min` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_nav_1min"
        )
        return self._api_call_to_json(call=call)

    def get_binned_1min_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_1min`.

        Args:
            instance (str, optional): The string to search for within
                `binned_1min`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_1min` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_1min"
        )
        return self._api_call_to_json(call=call)

    def get_binned_xmin_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_xmin`.

        Args:
            instance (str, optional): The string to search for within
                `binned_xmin`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_xmin` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_xmin"
        )
        return self._api_call_to_json(call=call)

    def get_binned_custom_nav_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_custom_nav`.

        Args:
            instance (str, optional): The string to search for within
                `binned_custom_nav`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_custom_nav` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_custom_nav"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_wind_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_default_wind`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_wind`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_wind` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_wind"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_wind_map_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_default_wind_map`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_wind_map`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_wind_map` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_wind_map"
        )
        return self._api_call_to_json(call=call)

    def get_binned_custom_wind_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_custom_wind`.

        Args:
            instance (str, optional): The string to search for within
                `binned_custom_wind`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_custom_wind` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_custom_wind"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_other_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_default_other`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_other`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_other` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_other"
        )
        return self._api_call_to_json(call=call)

    def get_binned_default_other_map_instance(
        self, instance: str = ""
    ) -> dict:
        """Gets a single instance of `binned_default_other_map`.

        Args:
            instance (str, optional): The string to search for within
                `binned_default_other_map`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_default_other_map` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_default_other_map"
        )
        return self._api_call_to_json(call=call)

    def get_binned_custom_other_instance(self, instance: str = "") -> dict:
        """Gets a single instance of `binned_custom_other`.

        Args:
            instance (str, optional): The string to search for within
                `binned_custom_other`. Defaults to "".

        Returns:
            dict: A custom dictionary containing the details of the
                `binned_custom_other` instance.
        """

        call = self._format_instance_call(
            instance=instance, call="binned_custom_other"
        )
        return self._api_call_to_json(call=call)


if __name__ == "__main__":
    coriolix = CoriolixAPI()
    pprint(coriolix.get_cruise_instance("1225 SEFIS"))
    pprint(coriolix.get_alert_action_list())
