import ast
import json
import sys

import edge_lake.generic.process_status as process_status
from external_lib.camera.camera import Camera
from external_lib.camera.support import rest_call, convert_xml, parse_logs


class AxisCamera(Camera):
    """
    Get geolocation of camera - used with self.get_configs
    """
    def _get_location(self, status)->(process_status, tuple):
        err_msg = ""
        location = (0.0, 0.0) # "unknown" location
        url = f"{self.base_url}/axis-cgi/geolocation/get.cgi"

        ret_val, response = rest_call(status=status, method='get', url=url, user=self.user, password=self.password,
                                      header={}, payload={})

        if not ret_val:
            try:
                decoded_content = response.content.decode() # decode content
                ret_val, updated_content = convert_xml(status, content=decoded_content) # convert XML to JSON
                if not ret_val and updated_content: # extract content
                    lat = updated_content.get('PositionResponse').get('Success').get('GetSuccess').get('Location').get('Lat')
                    long = updated_content.get('PositionResponse').get('Success').get('GetSuccess').get('Location').get('Long')
                    location = (ast.literal_eval(lat), ast.literal_eval(long))
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed to get geolocation for camera: {value}'
                ret_val = process_status.Uninitialized_variable

        if err_msg:
            status.add_error(err_msg)

        return [ret_val, location]

    """
    Based on the recording ID (optional) get information for the given recording(s)
    """
    def _get_recordings_info(self, status, recording_id:str='all')->(process_status, list):
        err_msg = ""
        url = f"{self.base_url}/axis-cgi/record/list.cgi?recordingid={recording_id}"
        ret_val, response = rest_call(status=status, method='get', url=url, user=self.user, password=self.password,
                                      header={}, payload={})
        recordings = []
        if not ret_val:
            try:
                decoded_content = response.content.decode()  # decode content
                ret_val, updated_content = convert_xml(status, content=decoded_content)  # convert XML to JSON
                if not ret_val and updated_content:
                    for user in list(updated_content.keys()):
                        if isinstance(updated_content[user].get('recordings'), list):
                            recordings += updated_content[user].get('recordings')
                        elif isinstance(updated_content[user].get('recordings'), dict):
                            recordings.append(updated_content[user].get('recordings'))
                        else:
                            status.add_error('Failed to extract recording information from camera')
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed to extract recording information from camera: {value}'
                ret_val = process_status.Uninitialized_variable

        if err_msg:
            status.add_error(err_msg)

        return [ret_val, recordings]


    #----------------------------#
    # Camera General information #
    #----------------------------#
    """
    Check camera status
    command: camera get status where name = [camera_name]
    """
    def camera_status(self, status)->(process_status, bool):
        """
        :return:
            if accessible returns 0, True
            else status_code, False
        """
        err_msg = ""
        is_running = False #  not accessible
        url = f"{self.base_url}/axis-cgi/systemready.cgi"
        headers = {
            "Content-Type": "application/json"
        }
        payload = {
            "apiVersion": "1.1",
            "context": "my context",
            "method": "systemready",
            "params": {"timeout": 20}
        }

        ret_val, response = rest_call(status=status, method='post', url=url, user=self.user, password=self.password,
                                      header=headers, payload=payload)

        if not ret_val:
            try:
                content = response.json()
                content_response = content.get('data').get('systemready') # if key(s) not found returns None
                if content_response and content_response.strip().lower() == 'yes':
                    is_running = True
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed to get camera status: {value}'
                ret_val = process_status.Failed_to_analyze_json

        if err_msg:
            status.add_error(f"{err_msg} - {ret_val}")

        return [ret_val, is_running]

    """
    Get camera configurations with geolocation 
    command: camera get configs where name=[camera_name]
    """
    def camera_configs(self, status):
        err_msg = ""
        url = f"{self.base_url}/axis-cgi/basicdeviceinfo.cgi"
        configs = {}
        headers = {
            "Content-Type": "application/json"
        }
        payload = {
            "apiVersion": "1.0",
            "context": "flask-client",
            "method": "getAllProperties"
        }

        ret_val, response = rest_call(status=status, method='post', url=url, user=self.user, password=self.password,
                                      header=headers, payload=payload)

        if not ret_val:
            try:
                content_dict = ast.literal_eval(response.content.decode())
                configs = content_dict.get('data').get('propertyList') # if key(s) not found returns None
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed to get configurations for camera: {value}'
                ret_val = process_status.Failed_to_analyze_json
            else:
                if configs:
                    ret_val, location = self._get_location(status=status)
                    if not ret_val:
                        configs['loc'] = location

        if err_msg:
            status.add_error(f"{err_msg} - {ret_val}")

        return [ret_val, configs]


    # ---------------------------#
    # Camera Recording Functions #
    # ---------------------------#
    """
    Get a list of recordings and their start / end timestamp value(s)
    get camera records where name=[xxx]
    :sample output: 
    {
        "recording_1": {"local_start_time": "2025-03-15 10:25:35", "local_end_time": "2025-03-15 12:32:45"}
    }       
    """
    def get_camera_recordings(self, status):
        ret_val, recordings = self._get_recordings_info(status=status, recording_id='all')
        recordings_obj = {}
        if not ret_val and recordings:
            for recording in recordings:
                recording_id = recording.get('@recordingid')
                if recording_id and recording_id not in recordings_obj:
                    recordings_obj[recording_id] = {
                        'local_start_time': recording.get('@starttimelocal'),
                        'local_end_time': recording_id.get('@@stoptimelocal')
                    }
                    
        if not recordings_obj:
            status.add_error('Failed to get list of recordings')
            if not ret_val:
                ret_val = process_status.Uninitialized_variable

        return [ret_val, recordings_obj]

    """
    Extract recording information for a specific recording ID
    camera get record where name=[camera name] and id=[recording id]
    :sample output:
    """
    def camera_recording_info(self, status, recording_id):
        ret_val, recording = self._get_recordings_info(status=status, recording_id=recording_id)
        if not recording:
            status.add_error(f'Failed to locate recording with ID {recording_id}')
            if not ret_val:
                ret_val = process_status.Uninitialized_variable

        return [ret_val, recording]

    """
    Extract video recording based on recording ID - format mp4
    camera get recording where name = [camera name] and id=[recording ID] 
    """
    def camera_recording_export(self, status, recording_id):
        err_msg = ""
        ret_val, recording_info = self.camera_recording_info(status=status, recording_id=recording_id) # get recording information
        content = None

        if not ret_val:
            diskid = None
            if isinstance(recording_info, dict):
                diskid = recording_info.get('diskid')
            elif isinstance(recording_info, list):
                diskid = recording_info[0].get('diskid')

            if diskid:
                url = f"{self.base_url}/axis-cgi/record/export/exportrecording.cgi?schemaversion=1&recordingid={recording_id}&exportformat=matroska&diskid={diskid}"
                ret_val, response = rest_call(status=status, method='get', url=url, user=self.user, password=self.password, timeout=120, stream=True)
                if not ret_val:
                    try:
                        content = response.content
                    except:
                        errno, value = sys.exc_info()[:2]
                        err_msg = f'Failed decode video content: {value}'
                        ret_val = process_status.Uninitialized_variable
            else:
                err_msg = f"Failed to get video for ID: {recording_id}"
                ret_val = process_status.Uninitialized_variable

        if err_msg:
            status.add_error(err_msg)

        return [ret_val, content]

    #----------------------------------#
    # To start a new recording process #
    # - stop recording                 #
    # - start recording                #
    #----------------------------------#
    """
    (re)Start camera process - for Axis this is how you force a new recording
    camera start recording where name = [camera name]  
    """
    def camera_recording_start(self, status):
        err_msg = ""
        output = 'unknown'
        url = f"{self.base_url}/axis/api/v1/recordings/start"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        payload = {"recordingId": "default"}

        ret_val, response = rest_call(status=status, method='post', url=url, user=self.user, password=self.password,
                                      header=headers, data=json.dumps(payload))

        if not ret_val:
            try:
                content_dict = response.json()
                if content_dict.get('data'):
                    output = content_dict.get('data').get('state')
                elif content_dict.get('state'):
                    output = content_dict.get('state')
                if output != 'recording':
                    err_msg =  f"Failed to start recording. Current state: {output}"
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed start camera: {value}'
                ret_val = process_status.Uninitialized_variable


        if err_msg or output != 'recording':
            if not err_msg:
                err_msg = f"Failed to start recording. Current state: {output}"
            status.add_error(err_msg)
            if not ret_val:
                ret_val = process_status.REST_call_err
        else:
            status.add_error('Warning: Camera started')

        return ret_val

    """
    Stop camera process - for Axis this is how you force a new recording
    camera stop recording where name = [camera name]  
    """
    def camera_recording_stop(self, status):
        err_msg = ""
        output = 'unknown'
        url = f"{self.base_url}/axis/api/v1/recordings/stop"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        payload = {"recordingId": "default"}

        ret_val, response = rest_call(status=status, method='post', url=url, user=self.user, password=self.password,
                                      header=headers, data=json.dumps(payload))

        if not ret_val:
            try:
                content_dict = response.json()
                if content_dict.get('data'):
                    output = content_dict.get('data').get('state')
                elif content_dict.get('state'):
                    output = content_dict.get('state')
                if output != 'stopped':
                    err_msg = f"Failed to stop recording. Current state: {output}"
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed start camera: {value}'
                ret_val = process_status.Uninitialized_variable

        if err_msg or output == 'unknown':
            if not err_msg:
                err_msg = f"Failed to stop recording. Current state: {output}"
            status.add_error(err_msg)
            if not ret_val:
                ret_val = process_status.REST_call_err
        else:
            status.add_error('Warning: Camera stopped')

        return ret_val

    # --------------------------#
    # Camera General Functions #
    # --------------------------#
    """
    Get (system) logs from camera  
    camera get logs where name=[camera name] 
    """
    def camera_logs(self, status):
        err_msg = ""
        parsed_logs = {}
        url = f"{self.base_url}/axis-cgi/admin/systemlog.cgi"
        ret_val, response = rest_call(status=status, method='get', url=url, user=self.user, password=self.password,
                                      header={}, payload={})

        if not ret_val:
            try:
                content = response.content.decode()
                log_text = content.strip().splitlines()
                ret_val, parsed_logs = parse_logs(status, log_text=log_text)
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed to extract logs from camera: {value}'
                ret_val = process_status.Uninitialized_variable

        if err_msg:
            status.add_error(err_msg)


        return ret_val, parsed_logs

    """
    Take a snapshot of the current video stream and store it  
    camera take snapshot where name=[name] and dest=[destination]  
    """
    def camera_snapshot(self, status, dest:str):
        err_msg = ""
        content = None
        url = f"{self.base_url}/axis-cgi/jpg/image.cgi"

        ret_val, response = rest_call(status=status, method='get', url=url, user=self.user, password=self.password,
                                      header={}, payload={}, stream=True)

        if not ret_val:
            try:
                content = response.content
                """
                write content to dest  - returned as base64 (jpeg) 
                """
            except:
                errno, value = sys.exc_info()[:2]
                err_msg = f'Failed to extract snapshot from camera: {value}'
                ret_val = process_status.Uninitialized_variable


        if err_msg:
            status.add_error(err_msg)

        return [ret_val, content]

