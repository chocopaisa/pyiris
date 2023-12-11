import os
import base64
import time
import json

from typing import (
    Union, List, Tuple, 
    Optional, Dict, Any, Sequence
)

from .iris_socket import IRISSocket
from .load import LoadOption
from .converter import convert
from .error import (
    OperationalError, 
    ProgrammingError, 
    DataError, 
    InternalError
)
import logging

logger = logging.getLogger(__name__)

class Cursor():
    """
    IRIS DB Cursor
    """

    record_sep:str = ''
    field_sep:str = ''

    def __init__(
            self, 
            socket:IRISSocket):
        self.is_initial_execution = False
        self.socket = socket

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
        debug_start_time = time.time()

        param = f"{user},{password},{host},{library_version}"
        encoded_param = base64.b64encode(param.encode('utf-8')).decode('utf-8')

        # send SETINFO command
        self.socket.send_message(f"SETINFO {encoded_param}\r\n")

        # result message from UDM
        is_success, msg = self.socket.read_message()
        if not is_success: 
            raise OperationalError(msg)

        debug_end_time = time.time()
        logger.debug("[DEBUG_TIME] SetInfo() %f" % (debug_end_time - debug_start_time))


    def set_buffer_size(self, size:int):
        self.buffer_size = size


    def _login(self, user:str, password:str, library_version:str) -> None:
        debug_start_time = time.time()

        param = f"{user},{password},{library_version}"
        encoded_param = base64.b64encode(param.encode('utf-8')).decode('utf-8')
        send_msg = f'LOGIN {encoded_param}\r\n'

        # send LOGIN command
        self.socket.send_message(send_msg)

        # welcome message from PGD
        is_success, msg = self.socket.read_message()
        if not is_success: 
            raise OperationalError(msg)

        # welcome message from UDM
        is_success, msg = self.socket.read_message()
        if not is_success: 
            raise OperationalError(msg)

        debug_end_time = time.time()
        logger.debug("[DEBUG_TIME] Login() %f" % (debug_end_time - debug_start_time))


    def _set_field_sep(self, sep:str) -> None:
        """
        Set Field seperator on Transaction
        """
        debug_start_time = time.time()
        encoded_sep = base64.b64encode(sep.encode('utf-8')).decode('utf-8')
        send_msg = f'SET_FIELD_SEP {encoded_sep}\r\n'
        self.socket.send_message(send_msg)
        is_success, msg = self.socket.read_message()
        if not is_success: 
            raise DataError(msg)

        self.field_sep = sep

        debug_end_time = time.time()
        logger.debug("[DEBUG_TIME] SetFieldSep() %f" % (debug_end_time - debug_start_time))


    def _set_record_sep(self, sep:str) -> None:
        """
        Set Record seperator on Transaction
        """
        debug_start_time = time.time()
        
        encoded_sep = base64.b64encode(sep.encode('utf-8')).decode('utf-8')
        send_msg = f"SET_RECORD_SEP {encoded_sep}\r\n"
        self.socket.send_message(send_msg)
        is_success, msg = self.socket.read_message()
        if not is_success: 
            raise DataError(msg)
        self.record_sep = sep

        debug_end_time = time.time()
        logger.debug("[DEBUG_TIME] SetRecordSep() %f" % (debug_end_time - debug_start_time))

    
    def _get_description(self) -> List[Tuple[Optional[str]]]:
        """
        name, type_code, display_size, internal_size, precision, scale, null_ok
        """
        self.socket.send_message("METADATA\r\n")
        is_success, msg = self.socket.read_message()
        if not is_success: 
            raise InternalError(msg)
        
        size = int(msg.strip())
        meta = []
        if size:
            metadata = self.socket.read(size)
            col_name_list, col_type_list = json.loads(metadata)
            for name, type in zip(col_name_list, col_type_list):
                meta.append((name, type, None, None, None, None, None))
        return meta
    

    def _has_semicolon(self, sql:str) -> bool:
        TARGET_COMMANDS = ("SELECT", "UPDATE", "INSERT", "DELETE", "CREATE", "DROP", "ALTER", "/*+")
        sql = sql.upper().strip()
        if sql.startswith(TARGET_COMMANDS) and not sql.endswith(";"):
            return False
        return True
    

    def _convert_params(self, args) -> Any:
        if isinstance(args, (tuple, list)):
            params = tuple([convert(a) for a in args])
        elif isinstance(args, dict):
            params = {k: convert(v) for k, v in args.items()}
        else:
            raise ProgrammingError("Not Supported argument type: %s" % type(args))
        
        return params
    
    def mogrify(
            self, 
            query:str, 
            args:Optional[Union[Sequence[Any], Dict[str, Any]]]=None
            ) -> str:
        if args is not None:
            params = self._convert_params(args)
            query = query % params
        
        return query
        

    def execute(self, operation:str, args:Optional[Union[Sequence[Any], Dict[str,Any]]]=None) -> str:
        """
        Execute Query
        
        execute2와 같은 함수
        기존 execute 함수는 execute1을 사용

        Params
        ------
        operation (str)
            : Query. 파라미터가 필요한 경우 %s 혹은 %(key)s 형식으로 작성.

        args (Optional[Union[Sequence[Any], Dict[str, Any]]])
            : Query에 들어갈 파라미터.

        Return
        ------
        str
            Result Message
        """
        self.has_next = False
        debug_start_time = time.time()
        operation = self.mogrify(operation, args)

        if not self._has_semicolon(operation):
            operation += ';'
            
        sql_size = len(operation.encode('utf-8'))
        self.socket.send_message(f"EXECUTE2 {sql_size}\r\n{operation}")

        is_success, msg = self.socket.read_message()
        if not is_success: 
            raise ProgrammingError(msg)
        
        self.description = self._get_description()
        self.is_initial_execution = False
        
        debug_end_time = time.time()
        logger.debug("[DEBUG_TIME] Execute2() %f" % (debug_end_time - debug_start_time))

        return msg
    
    
    def execute2(self, operation:str, args:Optional[Union[Sequence[Any], Dict[str,Any]]]=None) -> str:
        """
        Use method execute
        """
        return self.execute(operation, args)
    

    def execute1(self, operation:str) -> None:
        """
        기존 exeucte 함수
        """
        self.has_next = False
        self.is_initial_execution = False
        debug_start_time = time.time()
        if not self._has_semicolon(operation):
            operation += ";"
        sql_size = len(operation)

        self.socket.send_message(f"EXECUTE {sql_size}\r\n{operation}")
        self.is_initial_execution = True
        self.description = self._get_description()

        
        debug_end_time = time.time()
        logger.debug("[DEBUG_TIME] Execute() %f" % (debug_end_time - debug_start_time))
        

    def _read_data(self) :
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
        if "-" == tag[0] : 
            raise OperationalError(param)

        data = self.socket.read(int(param))
        self.buffer = json.loads(data)

        debug_end_time = time.time()
        logger.debug("[DEBUG_TIME] ReadData() %f" % (debug_end_time - debug_start_time))


    def fetchall(self): 
        debug_start_time = time.time()

        if not self.is_initial_execution : 
            self.socket.send_message("CONT ALL\r\n")
        else: 
            self.is_initial_execution = False

        tmp_list = []
        while True:
            msg = self.socket.readline()
            try: 
                tag, param = msg.strip().split(" ", 1) 
            except: 
                tag = msg.strip()

            if "+OK" == tag:
                break
            if "-" == tag[0]:
                raise OperationalError(param)

            tmp_list += json.loads(self.socket.read(int(param)))
            self.socket.send_message("CONT ALL\r\n")

        debug_end_time = time.time()
        logger.debug("[DEBUG_TIME] Fetchall() %f" % (debug_end_time - debug_start_time))

        return tmp_list


    def fetchone(self) -> tuple:
        if len(self.buffer) == 0: 
            self._read_data()
        record = self.buffer.pop(0)
        return record


    def load(
            self, 
            table:str, 
            load_file_path:str, 
            columns:Sequence[str], 
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
            raise DataError("-ERR file does not exist")

        # check control file is exists when normal loading.
        if not load_option.validate_csv and columns is None:
            raise DataError("-ERR column is not iterable type.\r\n")

        self._set_field_sep(sep)
        self._set_record_sep(record_sep)

        if columns:
            control_data = self.record_sep.join(columns)
            ctl_size = len(control_data)
        else:
            control_data = 'NULL'
            ctl_size = 4

        logger.debug(f"[DEBUG] GetSizeStart ({load_file_path})")
        data_size = os.path.getsize(load_file_path)
        logger.debug("[DEBUG] GetSizeEnd")

        send_msg = f"IMPORT {table},{key},{partition},{ctl_size},{data_size},{load_option}\r\n"

        self.socket.send_message(send_msg)
        self.socket.send_message(control_data)

        with open(load_file_path, newline='') as load_file:
            logger.debug("[DEBUG] OpenFile (%s)" % load_file_path)
            while True :
                buffer_data = load_file.read(self.buffer_size)
                if not buffer_data:
                    break #EOF
                self.socket.send_message(buffer_data)
            logger.debug("[DEBUG] End (%s)" % load_file_path)

        is_success, msg = self.socket.read_message()
        if not is_success: 
            raise ProgrammingError(msg)


    def close(self):
        pass

    def __del__(self):
        self.close()

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type:type, exc_value: Exception, traceback:type) -> None:
        self.close()



class DictCursor(Cursor):
    """
    Dict Cursor

    반환 결과가 tuple(혹은 list) 가 아닌 dictionary 형태
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    # override
    def fetchall(self) -> Tuple[Dict[str, Any]]:
        datas = super().fetchall()
        result = []
        for data in datas:
            result.append(self._map_column(data))

        return result
    
    # override
    def fetchone(self) -> tuple:
        data = super().fetchone()
        return self._map_column(data)
    
    def _map_column(self, data:tuple) -> dict:
        result = {}
        for col_desc, col_value in zip(self.description, data):
            result[col_desc[0]] = col_value

        return result

        

        
        
