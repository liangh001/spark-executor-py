"Core exceptions raised by the spark executor"


class SparkError(Exception):
    pass

class SystemCommandExecuteError(Exception):
    pass

class SparkUnfoundError(SparkError):
    pass

class SparkSqlUnfoundError(SparkError):
    pass

class SparkCommandExecuteError(SparkError):
    pass

class SparkSqlCommandExecuteError(SparkError):
    pass
    
class ConnectionError(SparkError):
    pass


class TimeoutError(SparkError):
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(SparkError):
    pass


class ResponseError(SparkError):
    pass


class DataError(SparkError):
    pass


class PubSubError(SparkError):
    pass


class WatchError(SparkError):
    pass


class NoScriptError(ResponseError):
    pass


class ExecAbortError(ResponseError):
    pass


class ReadOnlyError(ResponseError):
    pass
