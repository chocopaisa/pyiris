import socket
import struct
import errno
from typing import List, Tuple, Union, Optional

class SocketDisconnectException(Exception): pass
class SocketDataSendException(Exception): pass
class SocketTimeoutException(Exception): pass

class IRISSocket():
    """
    IRIS Socket
    
    >>> my_socket = IRISSocket()
    >>> my_socket.connect(my_ip, my_port)
    """

    def __init__(self) -> None:
        self.socket:socket.socket = None
        self.remaining_buffer_size:int = 0
        self.received_data_list:List[str] = []
        self.remained_data = ""
        self.timeout:Optional[Union[int, float]] = None

    def connect(self, ip:str, port: int) -> None:
        """
        Connect socket to Server

        Params
        ------
        ip (str)
            : 연결할 IP 혹은 Host
        port (int)
            : 연결할 Port 번호
        """
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        linger = struct.pack("ii", 1, 0)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, linger)
        self.socket.connect((ip, port))


    def set_timeout(self, timeout:Union[int, float]) -> None:
        """
        Set timeout

        Params
        ------
        timeout (Union[int, float])
            : 타임 아웃 시간(초)
        """
        self.timeout = timeout
        if self.socket:
            self.socket.settimeout(timeout)

    def readline(
            self, 
            continue_on_empty:bool=True, 
            timeout:Union[int, float]=0
            ) -> str:
        """
        Read message from Server (1 Line)

        Params
        ------
        continue_on_empty (bool)
            : 데이터가 없는 경우에 재시도
        Return 
        ------
        str (data)
            : 서버 응답 메시지(1줄)
        """
        if timeout > 0:
            self.socket.settimeout(timeout)
        else:
            self.socket.settimeout(self.timeout)
        data = self._readline(continue_on_empty)
        self.socket.settimeout(self.timeout)
        return data

    def read(self, size:int, timeout:Union[int, float]=0) -> str:
        """
        Read message from Server (size)

        Params
        ------
        size (int)
            : 수신 받을 버퍼 크기

        Return 
        ------
        str
            : 서버 응답 메시지
        """
        if timeout > 0:
            self.socket.settimeout(timeout)
        else:
            self.socket.settimeout(self.timeout)
        data = self._read(size)
        self.socket.settimeout(self.timeout)
        return data

    def send_message(
            self, 
            cmd:str, 
            timeout:Union[int, float]=0
            ) -> None:
        """
        Send message to Server

        Params
        ------
        cmd (str)
            : 전송할 command
        timeout (Union[int, float])
            : 소켓 timeout 시간
        """
        if timeout > 0:
            timeout *= 3
        else:
            if self.timeout == None:
                timeout = None
            else:
                timeout = self.timeout * 3

        self.socket.settimeout(timeout)
        self._send_message(cmd)
        self.socket.settimeout(self.timeout)

    def _readline(self, continue_on_empty:bool=True) -> str:
        """
        서버로 부터 메시지 수신 (1줄)

        Params
        ------
        continue_on_empty (bool)
            : 데이터가 없는 경우에 재시도 여부
        Return 
        ------
        str (data)
            : 서버 응답 메시지(1줄)
        """
        data = ""

        temp_data_buffer = self.remained_data
        temp_socket = self.socket

        if "\n" in temp_data_buffer:
            data, self.remained_data = temp_data_buffer.split("\n", 1)
            self.remained_data = temp_data_buffer
            self.socket = temp_socket
            return data
        
        while True:
            try:
                received_data = temp_socket.recv(2048000).decode("utf-8")
            except socket.timeout:
                self.remained_data = temp_data_buffer
                self.socket = temp_socket
                raise SocketTimeoutException
            except socket.error as e:
                if e.args[0] == errno.ECONNRESET:
                    self.remained_data = temp_data_buffer
                    self.socket = temp_socket
                    raise SocketDisconnectException

            if not received_data:
                self.remained_data = temp_data_buffer
                self.socket = temp_socket
                raise SocketDisconnectException
            
            temp_data_buffer = temp_data_buffer + received_data

            if "\n" in received_data:
                data, temp_data_buffer = temp_data_buffer.split("\n", 1)
                break

            if not continue_on_empty:
                break

        self.remained_data = temp_data_buffer
        self.socket = temp_socket

        return data

    def _read(self, size:int) -> str:
        """
        서버로 부터 메시지 수진 (size만큼)

        Params
        ------
        size (int)
            : 수신 받을 버퍼 크기

        Return 
        ------
        str
            : 서버 응답 메시지
        """
        temp_socket = self.socket
        temp_data_buffer = self.remained_data

        remaining_bytes = size
        received_data_list:List[str] = []

        if not temp_data_buffer:
            return ""

        if remaining_bytes <= len(temp_data_buffer):
            self.remained_data = temp_data_buffer[remaining_bytes:]
            return temp_data_buffer[:remaining_bytes]
        
        remaining_bytes -= len(temp_data_buffer)
        received_data_list.append(temp_data_buffer)
        temp_data_buffer = ""
        while 1:
            received_data = ""
            try:
                received_data = temp_socket.recv(remaining_bytes).decode("utf-8")
            except socket.timeout:
                self.socket = temp_socket
                self.remaining_buffer_size = remaining_bytes
                self.remained_data = temp_data_buffer
                self.received_data_list = received_data_list
                raise SocketTimeoutException
            except socket.error as e:
                if e.args[0] == errno.ECONNRESET:
                    self.socket = temp_socket
                    self.remaining_buffer_size = remaining_bytes
                    self.remained_data = temp_data_buffer
                    self.received_data_list = received_data_list
                    raise SocketDisconnectException

            if received_data == "":
                self.socket = temp_socket
                self.remaining_buffer_size = remaining_bytes
                self.remained_data = temp_data_buffer
                self.received_data_list = received_data_list
                raise SocketDisconnectException
            
            received_data_list.append(received_data)
            remaining_bytes -= len(received_data)
            if remaining_bytes <= 0:
                break

        self.socket = temp_socket
        self.remaining_buffer_size = 0
        self.remained_data = temp_data_buffer
        self.received_data_list = []
        return "".join(received_data_list)

    def read_message(self) -> Tuple[bool, str]:
        """
        Read message from Server
        
        Return
        ------
        Tuple[bool, str]
            is success, server message
        """
        SUCCESS_CODE = "+"
        line = self.readline()
        if " " in line:
            code, msg = line.split(" ", 1)
        else:
            code, msg = line, ''
        if code[0] == SUCCESS_CODE:
            return True, msg
        return False, msg
    

    def _send_message(self, cmd:str) -> None:
        """
        Send Message to Server

        Params
        ------
        cmd (str)
            : 전송할 command
        
        """
        while True:
            try:
                served_buffer_size = self.socket.send(cmd.encode("utf-8"))
            except socket.timeout:
                raise SocketTimeoutException
            except socket.error as e:
                if e.args[0] == errno.ECONNRESET:
                    raise SocketDisconnectException

            if served_buffer_size == len(cmd):
                break
            elif served_buffer_size <= 0:
                self.socket.settimeout(None)
                raise SocketDataSendException
            
            cmd = cmd[served_buffer_size:]

    def close(self) -> None:
        """
        Close Socket
        """
        if self.socket:
            self.socket.close()
            self.socket = None
