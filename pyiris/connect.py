import time
from typing import (
    Tuple, Union, Optional, Type
)
from .cursor import Cursor
from .iris_socket import IRISSocket
from .error import OperationalError
import logging

logger = logging.getLogger(__name__)

VERSION = "Python-M6-API-2.1"
DEFAULT_CURSOR = Cursor

class Connection() :
    """
    IRIS Connection
    """

    def __init__(
            self, 
            *,
            host:Optional[str] = None, 
            user:Optional[str] = None, 
            password:str = "", 
            database:Optional[str] = None,
            port:int = 0,
            direct_mode_enabled:bool = False, 
            timeout:Union[int,float] = 0,
            cursor_class_:Type[Cursor] = DEFAULT_CURSOR
        ) -> None:
        self.host = host or ""
        self.port = port or 0
        self.user = user or ""
        self.password = password  or ""
        self.direct_mode_enabled = direct_mode_enabled
        self.timeout = timeout or 0
        self.database = database
        self.cursor_class_ = cursor_class_

        self.cursor_: Optional[Cursor] = None
        self.socket = IRISSocket()

        if self.timeout > 0:
            self.socket.set_timeout(self.timeout)

        # connect
        self._connect()

    def _connect(self) -> None:
        """Connect to Server"""
        debugStartTime = time.time()

        self._connect_server(self.host, self.port)

        if self.direct_mode_enabled :
            udm_ip, udm_port = self._nsd_connect()
            self._connect_server(udm_ip, udm_port)

        debugEndTime = time.time()
        logger.debug("[DEBUG_TIME] Connect() %f") % (debugEndTime - debugStartTime)
        

    def _connect_server(self, host:str, port:int) -> None:
        """Connect to Server"""
        if not self.socket : 
            self.socket = IRISSocket()

        if self.timeout > 0:
            self.socket.set_timeout(self.timeout)

        try:
            self.socket.connect(host, port)
        except Exception as e:
            self.close()
            raise OperationalError(f"Unable to connect to server.[{e}]")
            
        # Read welcome message
        result, msg = self.socket.read_message()
        if not result:
            self.close()
            raise OperationalError("Unable to readMessage. sock")


    def _nsd_connect(self) -> Tuple[Union[str,int]]:
        self.socket.send_message("GET\r\n")
        result, msg = self.socket.read_message()
        if not result :
            if msg.strip() == "Invalid Command":
                raise OperationalError("For DIRECT Connection, IRIS NSD PORT is required, but invalid port is given.")
            raise OperationalError(msg)

        ip = msg.strip().split(":", 1)[0]
        port = int(msg.strip().split(":", 1)[1])

        self.socket.send_message("QUIT\r\n")

        try: self.socket.readline() # NSD : OK BYE
        except: pass
        try: self.socket.close()
        except: pass    
        self.socket = None
        return ip, port


    def cursor(self) -> Cursor:
        self.cursor_ = self.cursor_class_(socket=self.socket)

        if self.direct_mode_enabled:
            host = self.socket.socket.getsockname()[0]
            self.cursor_._set_info(self.user, self.password, host, VERSION)
        else:
            self.cursor_._login(self.user, self.password, VERSION)

        if self.database:
            self.cursor_.execute(f"USE {self.database};")

        return self.cursor_

    def commit(self) -> None:
        pass

    def close(self) -> None:
        self.commit()
        if self.cursor_:
            self.cursor_.close()

        try: self.socket.send_message("QUIT\r\n", timeOut=1)
        except: pass
        try: self.socket.readline(timeOut=1)
        except: pass
        try: self.socket.close()
        except: pass
        self.socket = None

    def __del__(self):
        self.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type:type, exc_value: Exception, traceback: type) -> None:
        self.close()
