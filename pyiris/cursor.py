import os
import base64
import time
import json

from typing import Union, List, Tuple, Optional, Dict, Any
from overrides import overrides

from .iris_socket import IRISSocket
from .load import LoadOption
from .error import (
    OperationalError, 
    ProgrammingError, 
    DataError, 
    InternalError
)


class Cursor():
    record_sep:str = ''
    field_sep:str = ''
    def __init__(
            self, 
            socket:IRISSocket, 
            debug_mode_enabled:bool=False):
        self.is_initial_execution = False
        self.socket = socket
        self.debug_mode_enabled = debug_mode_enabled

        self.buffer:list = []
        self.process_status:str = ""
        self.buffer_size:int = 1024
        self.has_next:bool = False

    def __iter__(self):        
        return self    

    def next(self) -> tuple:        
        return self.fetchone()

    def __next__(self) -> tuple:
        return self.next()

    def set_timeout(self, timeout:Union[int, float]) -> None:
        self.socket.set_timeout(timeout)
        
    def _set_info(self, user:str, password:str, host:str, library_version:str) -> None:
        if self.debug_mode_enabled:
            debug_start_time = time.time()

        param = f"{user},{password},{host},{library_version}"
        encoded_param = base64.b64encode(param.encode('utf-8')).decode('utf-8')
        send_msg = f"SETINFO {encoded_param}\r\n"

        # send SETINFO command
        self.socket.send_message(send_msg)

        # result message from UDM
        is_success, msg = self.socket.read_message()
        if not is_success: 
            raise OperationalError(msg)

        if self.debug_mode_enabled:
            debug_end_time = time.time()
            print("[DEBUG_TIME] SetInfo() %f" % (debug_end_time - debug_start_time))


    def set_buffer_size(self, size:int):
        self.buffer_size = size

    def _login(self, user:str, password:str, library_version:str) -> None:
        if self.debug_mode_enabled:
            debug_start_time = time.time()

        param = f"{user},{password},{library_version}"
        encoded_param = base64.b64encode(param.encode('utf-8')).decode('utf-8')
        send_msg = f'LOGIN {encoded_param}\r\n'

        # send LOGIN command
        self.socket.send_message(send_msg)

        # welcome message from PGD
        is_success, msg = self.socket.read_message()
        if not is_success: raise OperationalError(msg)

        # welcome message from UDM
        is_success, msg = self.socket.read_message()
        if not is_success: raise OperationalError(msg)

        if self.debug_mode_enabled:
            debug_end_time = time.time()
            print("[DEBUG_TIME] Login() %f" % (debug_end_time - debug_start_time))

    def _set_field_sep(self, sep:str) -> None:
        if self.debug_mode_enabled:
            debug_start_time = time.time()
        encoded_sep = base64.b64encode(sep.encode('utf-8')).decode('utf-8')
        send_msg = f'SET_FIELD_SEP {encoded_sep}\r\n'
        self.socket.send_message(send_msg)
        is_success, msg = self.socket.read_message()
        if not is_success: raise DataError(msg)

        self.field_sep = sep

        if self.debug_mode_enabled:
            debug_end_time = time.time()
            print("[DEBUG_TIME] SetFieldSep() %f" % (debug_end_time - debug_start_time))


    def _set_record_sep(self, sep:str) -> None:
        if self.debug_mode_enabled:
            debug_start_time = time.time()
        
        encoded_sep = base64.b64encode(sep.encode('utf-8')).decode('utf-8')
        send_msg = f"SET_RECORD_SEP {encoded_sep}\r\n"
        self.socket.send_message(send_msg)
        is_success, msg = self.socket.read_message()
        if not is_success: raise DataError(msg)
        self.record_sep = sep

        if self.debug_mode_enabled:
            debug_end_time = time.time()
            print("[DEBUG_TIME] SetRecordSep() %f" % (debug_end_time - debug_start_time))

    
    def _get_description(self) -> List[Tuple[Optional[str]]]:
        """
        name, type_code, display_size, internal_size, precision, scale, null_ok
        """
        sendMsg = "METADATA\r\n"
        self.socket.send_message(sendMsg)
        is_success, msg = self.socket.read_message()
        if not is_success: raise InternalError(msg)
        size = int(msg.strip())
        meta = []
        if size:
            metadata = self.socket.read(size)
            col_name_list, col_type_list = json.loads(metadata)
            for name, type in zip(col_name_list, col_type_list):
                meta.append((name, type, None, None, None, None, None))
        return meta
    

    def _check_semi(self, sql:str) -> str:
        chk_sql = sql.upper().strip()
        if chk_sql.startswith("SELECT") \
                or chk_sql.startswith("UPDATE") \
                or chk_sql.startswith("INSERT") \
                or chk_sql.startswith("DELETE") \
                or chk_sql.startswith("CREATE") \
                or chk_sql.startswith("DROP") \
                or chk_sql.startswith("ALTER") \
                or chk_sql.startswith("/*+"):
            if not chk_sql.endswith(";"):
                return sql + ";"
        return sql
    

    def execute(self, operation:str) -> str:
        """
        Execute Query

        Params
        ------
        operation (str)
            : Query

        Return
        ------
        str
            Result Message
        """
        self.has_next = False
        if self.debug_mode_enabled:
            debug_start_time = time.time()
        if not operation.endswith(';'):
            operation += ';'

        sql_size = len(operation.encode('utf-8'))

        send_msg = f"EXECUTE2 {sql_size}\r\n{operation}"
        self.socket.send_message(send_msg)

        is_success, msg = self.socket.read_message()
        if not is_success: raise ProgrammingError(msg)
        self.is_initial_execution = False

        if self.debug_mode_enabled:
            debug_end_time = time.time()
            print("[DEBUG_TIME] Execute2() %f" % (debug_end_time - debug_start_time))

        self.description = self._get_description()

        return msg

    def execute1(self, operation:str) -> None:
        self.has_next = False
        self.is_initial_execution = False
        if self.debug_mode_enabled:
            debug_start_time = time.time()

        operation = self._check_semi(operation)
        sql_size = len(operation)

        sendMsg = f"EXECUTE {sql_size}\r\n{operation}"
        self.socket.send_message(sendMsg)
        self.is_initial_execution = True

        if self.debug_mode_enabled:
            debug_end_time = time.time()
            print("[DEBUG_TIME] Execute() %f" % (debug_end_time - debug_start_time))
        
        self.description = self._get_description()

    def _read_data(self) :
        if self.debug_mode_enabled:
            debug_start_time = time.time()

        if not self.is_initial_execution : 
            self.socket.send_message("CONT\r\n")
        else: 
            self.is_initial_execution = False

        msg = self.socket.readline()
        try: (tag, param) = msg.strip().split(" ", 1) 
        except: tag = msg.strip()

        if "+OK" == tag :
            self.has_next = True
            raise StopIteration()
        if "-" == tag[0] : raise OperationalError(param)

        data = self.socket.read(int(param))
        self.buffer = json.loads(data)

        if self.debug_mode_enabled:
            debug_end_time = time.time()
            print("[DEBUG_TIME] ReadData() %f" % (debug_end_time - debug_start_time))


    def fetchall(self): 
        if self.debug_mode_enabled:
            debug_start_time = time.time()

        if not self.is_initial_execution : 
            self.socket.send_message("CONT ALL\r\n")
        else: 
            self.is_initial_execution = False

        tmp_list = []
        while True:
            msg = self.socket.readline()
            try: (tag, param) = msg.strip().split(" ", 1) 
            except: tag = msg.strip()

            if "+OK" == tag:
                break
            if "-" == tag[0]:
                raise OperationalError(param)

            tmp_list += json.loads(self.socket.read(int(param)))

            self.socket.send_message("CONT ALL\r\n")

        if self.debug_mode_enabled:
            debug_end_time = time.time()
            print("[DEBUG_TIME] Fetchall() %f" % (debug_end_time - debug_start_time))

        return tmp_list


    def fetchone(self) -> tuple:
        if len(self.buffer) == 0: 
            self._read_data()
        record = self.buffer.pop(0)
        return record
    

    def _logger(self, msg):
        if self.debug_mode_enabled:
            print(msg)


    def load(
            self, 
            table:str, 
            load_file_path:str, 
            columns:List[str], 
            key:str="",
            partition:str="",
            sep:str=",",
            record_sep:str="\n",
            skip_rows:int=0,
            use_zlib:bool=False
            ) -> None:

        load_option = LoadOption(
            skip_rows=skip_rows, 
            use_zlib=use_zlib
        )

        if not os.path.exists(load_file_path):
            raise DataError("-ERR dat is invalid.\r\n")

        # check control file is exists when normal loading.
        if not load_option.validate_csv and columns is None:
            raise DataError("-ERR ctl file doesn't exist.\r\n")

        if columns:
            control_data = self.record_sep.join(columns)
            ctl_size = len(control_data)
        else:
            control_data = 'NULL'
            ctl_size = 4

        self._set_field_sep(sep)
        self._set_record_sep(record_sep)

        self._logger(f"[DEBUG] GetSizeStart ({load_file_path})")
        data_size = os.path.getsize(load_file_path)
        self._logger("[DEBUG] GetSizeEnd")

        send_msg = f"IMPORT {table},{key},{partition},{ctl_size},{data_size},{load_option}\r\n"

        self.socket.send_message(send_msg)
        self.socket.send_message(control_data)

        with open(load_file_path, newline='') as load_file:
            self._logger("[DEBUG] OpenFile (%s)" % load_file_path)
            while True :
                buffer_data = load_file.read(self.buffer_size)
                if not buffer_data:
                    break #EOF
                self.socket.send_message(buffer_data)

            self._logger("[DEBUG] End (%s)" % load_file_path)


    def close(self):
        pass

    def __del__(self):
        self.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type:type, exc_value: Exception, traceback:type) -> None:
        self.close()



class DictCursor(Cursor):

    def __init__(
            self, 
            socket:IRISSocket, 
            debug_mode_enabled:bool=False):
        super().__init__(socket, debug_mode_enabled)

    @overrides
    def fetchall(self) -> Tuple[Dict[str, Any]]:
        datas = super().fetchall()
        result = []
        for data in datas:
            result.append(self._map_column(data))

        return result
    
    @overrides
    def fetchone(self) -> tuple:
        data = super().fetchone()
        
        return self._map_column(data)
    
    def _map_column(self, data:tuple) -> dict:
        result = {}
        for col_desc, col_value in zip(self.description, data):
            result[col_desc[0]] = col_value

        return result

        

        
        
