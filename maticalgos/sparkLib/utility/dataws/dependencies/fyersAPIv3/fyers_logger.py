from typing import Any, Dict, Union
from aws_lambda_powertools import Logger


class FyersLogger(Logger):
    def __init__(
        self, service: str, level: str, stack_level: int = 4, **kwargs
    ) -> None:
        """Create FyersLogger object

        Args:
            service (str): Service name. This should be the same across the code wherever the logger is initialized
            level (str): Logger level. Possible values are [INFO, DEBUG, CRITICAL, WARNING]

        Kwargs:
            stack_level (int): Stack level. This decides how many levels the stack should go up to get the line number
                            to be printed in the logging statement
        """
        super().__init__(
            service=service,
            level=level,
            location="[%(funcName)s:%(lineno)s] %(module)s",
            **kwargs
        )
        self.__stacklevel = stack_level

    def __populate_request_data(self, stack_level, **kwargs) -> Dict[str, Any]:
        """Adds additional log data to log statement

        Returns:
            Dict[str, Any]: all the keyword arguments along with extra data
        """
        kwargs["stacklevel"] = stack_level
        if "extra" not in kwargs:
            kwargs["extra"] = {}

        if "message" in kwargs["extra"]:
            kwargs["extra"]["passed_message"] = kwargs["extra"].pop("message")
        return kwargs

    def error(self, msg: Union[str, Dict[Any, Any]], *args, **kwargs) -> None:
        """Logs error statement

        Args:
            msg (Union[str, Dict[Any, Any]]): Can be str or dict object

        Kwargs:
            extra (Dict[Any, Any]): Adds this data to the log statement
        """
        stacklevel = self.__stacklevel
        while stacklevel > 0:
            try:
                kwargs = self.__populate_request_data(stacklevel, **kwargs)
                super().error(msg, *args, **kwargs)
                break
            except:
                stacklevel -= 1

    def info(self, msg: Union[str, Dict[Any, Any]], *args, **kwargs) -> None:
        """Logs info statement

        Args:
            msg (Union[str, Dict[Any, Any]]): Can be str or dict object

        Kwargs:
            extra (Dict[Any, Any]): Adds this data to the log statement
        """
        stacklevel = self.__stacklevel
        while stacklevel > 0:
            try:
                kwargs = self.__populate_request_data(stacklevel, **kwargs)
                super().info(msg, *args, **kwargs)
                break
            except:
                stacklevel -= 1

    def debug(self, msg: Union[str, Dict[Any, Any]], *args, **kwargs) -> None:
        """Logs debug statement

        Args:
            msg (Union[str, Dict[Any, Any]]): Can be str or dict object

        Kwargs:
            extra (Dict[Any, Any]): Adds this data to the log statement
        """
        stacklevel = self.__stacklevel
        while stacklevel > 0:
            try:
                kwargs = self.__populate_request_data(stacklevel, **kwargs)
                super().debug(msg, *args, **kwargs)
                break
            except:
                stacklevel -= 1

    def exception(self, msg: Union[str, Dict[Any, Any]], *args, **kwargs) -> None:
        """Logs exception statement. Should be called only from exception block

        Args:
            msg (Union[str, Dict[Any, Any]]): Can be str or dict object

        Kwargs:
            extra (Dict[Any, Any]): Adds this data to the log statement
        """
        stacklevel = self.__stacklevel
        while stacklevel > 0:
            try:
                kwargs = self.__populate_request_data(stacklevel, **kwargs)
                super().exception(msg, *args, **kwargs)
                break
            except:
                stacklevel -= 1
