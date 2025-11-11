from abc import ABC, abstractmethod
import anylog_node.generic.process_status as process_status

class Camera(ABC):
    def __init__(self, base_url:str, user:str=None, password:str=None):
        """
        Connection to camera in order to execute commands
        """
        self.base_url = base_url
        self.user = user
        self.password = password

    #----------------------------#
    # Camera General information #
    #----------------------------#
    """
    Get camera status 
    camera get status where name = [camera name] 
    """
    @abstractmethod
    def camera_status(self, status):
        """
        :return:
            if accessible returns 0, True
            else status_code, False
        """
        ret_val = process_status.NotImplementedError
        return [ret_val, None]

    """
    Get camera configurations
    camera get configs where name = [camera name] 
    """
    @abstractmethod
    def camera_configs(self, status):
        ret_val = process_status.NotImplementedError
        return [ret_val, None]

    #----------------------------#
    # Camera Recording Functions #
    #----------------------------#
    """
    Get list of recordings 
    camera get recordings where name = [camera name] 
    """
    @abstractmethod
    def camera_recordings(self, status):
        ret_val = process_status.NotImplementedError
        return [ret_val, None]

    """
    Get recording information
    camera get recording info where name = [camera name] and id = [recording ID] 
    """
    @abstractmethod
    def camera_recording_info(self, status, recording_id):
        ret_val = process_status.NotImplementedError
        return [ret_val, None]

    """
    Get the actual recording
    camera get recording where name = [camera name] and id = [recording ID]
    """
    @abstractmethod
    def camera_recording_export(self, status, recording_id):
        ret_val = process_status.NotImplementedError
        return [ret_val, None]

    # ----------------------------------#
    # To start a new recording process #
    # - stop recording                 #
    # - start recording                #
    # ----------------------------------#
    """
    (re)Start camera process - for Axis this is how you force a new recording
    camera start recording where name = [camera name]  
    """
    @abstractmethod
    def camera_recording_start(self, status):
        ret_val = process_status.NOT_SUPPORTED  # there could be cameras that do not support this function
        return ret_val

    """
    Stop camera process - for Axis this is how you force a new recording
    camera stop recording where name = [camera name]  
    """
    @abstractmethod
    def camera_recording_stop(self, status):
        ret_val = process_status.NOT_SUPPORTED  # there could be cameras that do not support this function
        return ret_val


    #--------------------------#
    # Camera General Functions #
    #--------------------------#
    """
    Get (system) logs from camera  
    camera get log where name = [camera name] 
    """
    @abstractmethod
    def camera_logs(self, status):
        ret_val = process_status.NOT_SUPPORTED # there could be cameras that do not support this function
        return [ret_val, None]

    """
    Take a snapshot of the current video stream and store it
    camera take snapshot where name=[name] and dest=[destination]  
    """
    def camera_snapshot(self, status, dest:str):
        ret_val = process_status.NOT_SUPPORTED # there could be cameras that do not support this function
        return [ret_val, None]


