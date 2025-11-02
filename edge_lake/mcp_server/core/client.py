"""
EdgeLake HTTP Client

Multi-threaded async client for EdgeLake REST API communication.

License: Mozilla Public License 2.0
"""

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

try:
    import requests
    from requests.exceptions import HTTPError, RequestException
except ImportError:
    # If requests not available, use urllib as fallback
    import urllib.request
    import urllib.error
    requests = None

logger = logging.getLogger(__name__)


class EdgeLakeClient:
    """
    Async HTTP client for EdgeLake REST API with thread pool execution.
    """
    
    def __init__(self, host: str, port: int, timeout: int = 20, max_workers: int = 10):
        """
        Initialize EdgeLake client.
        
        Args:
            host: EdgeLake node IP/hostname
            port: EdgeLake REST API port (typically 32049)
            timeout: Request timeout in seconds
            max_workers: Maximum concurrent worker threads
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        self.base_url = f"http://{host}:{port}"
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        logger.debug(f"EdgeLake client initialized: {self.base_url}")
    
    def _execute_request_with_requests(self, method: str, command: str, 
                                       headers: Optional[Dict[str, str]] = None) -> Any:
        """Execute request using requests library"""
        request_headers = {
            "User-Agent": "anylog",
            "command": command,
        }
        
        if headers:
            request_headers.update(headers)
        
        logger.debug(f"Request: {method} {self.base_url}, command='{command}'")
        
        try:
            if method.upper() == "GET":
                response = requests.get(
                    url=self.base_url,
                    headers=request_headers,
                    timeout=self.timeout,
                    verify=False
                )
            elif method.upper() == "POST":
                response = requests.post(
                    url=self.base_url,
                    headers=request_headers,
                    timeout=self.timeout,
                    verify=False
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            if response.status_code != 200:
                error_msg = f"EdgeLake returned status {response.status_code}"
                if hasattr(response, 'text'):
                    error_msg += f": {response.text}"
                raise Exception(error_msg)
            
            # Try to parse as JSON
            try:
                return response.json()
            except ValueError:
                return response.text
                
        except HTTPError as e:
            logger.error(f"HTTP error: {e}")
            raise Exception(f"HTTP error: {str(e)}")
        except RequestException as e:
            logger.error(f"Request error: {e}")
            raise Exception(f"Request error: {str(e)}")
    
    def _execute_request_with_urllib(self, method: str, command: str,
                                     headers: Optional[Dict[str, str]] = None) -> Any:
        """Execute request using urllib as fallback"""
        request_headers = {
            "User-Agent": "anylog",
            "command": command,
        }
        
        if headers:
            request_headers.update(headers)
        
        logger.debug(f"Request (urllib): {method} {self.base_url}, command='{command}'")
        
        try:
            req = urllib.request.Request(
                self.base_url,
                headers=request_headers,
                method=method.upper()
            )
            
            with urllib.request.urlopen(req, timeout=self.timeout) as response:
                data = response.read().decode('utf-8')
                
                # Try to parse as JSON
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    return data
                    
        except urllib.error.HTTPError as e:
            error_msg = f"HTTP error {e.code}: {e.reason}"
            logger.error(error_msg)
            raise Exception(error_msg)
        except urllib.error.URLError as e:
            logger.error(f"URL error: {e.reason}")
            raise Exception(f"URL error: {e.reason}")
    
    def _execute_request(self, method: str, command: str,
                        headers: Optional[Dict[str, str]] = None) -> Any:
        """
        Execute HTTP request to EdgeLake (synchronous, runs in thread pool).
        
        Args:
            method: HTTP method (GET, POST)
            command: EdgeLake command to execute
            headers: Optional additional headers
        
        Returns:
            Response data (parsed JSON or text)
        
        Raises:
            Exception: On request failure
        """
        if requests is not None:
            return self._execute_request_with_requests(method, command, headers)
        else:
            return self._execute_request_with_urllib(method, command, headers)
    
    async def _async_request(self, method: str, command: str,
                             headers: Optional[Dict[str, str]] = None) -> Any:
        """
        Execute HTTP request asynchronously using thread pool.
        
        Args:
            method: HTTP method
            command: EdgeLake command
            headers: Optional headers
        
        Returns:
            Response data
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            self._execute_request,
            method,
            command,
            headers
        )
    
    async def execute_command(self, command: str, method: str = "GET",
                             headers: Optional[Dict[str, str]] = None) -> Any:
        """
        Execute an EdgeLake command.

        Args:
            command: EdgeLake command string
            method: HTTP method (default: GET)
            headers: Optional headers

        Returns:
            Response data
        """
        return await self._async_request(method, command, headers)

    def close(self):
        """Shutdown the thread pool executor"""
        logger.debug("Shutting down EdgeLake client")
        self.executor.shutdown(wait=True)
